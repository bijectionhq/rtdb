//! An inner equi-join of two batch inputs on column equalities.
//! It builds its left input in memory and emits pages of columns, matching the row join's output row for row:
//! build rows are hashed in input order into the same index, and each probe row takes its candidates in that order.
use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::Array;
use rtdb_columnar::MemoryReservation;
use rtdb_plan::{BinaryOp, Expr};
use rtdb_types::{Result, Value};

use crate::columnar::{BatchCursor, BoxBatchCursor, Column, ColumnBatch, gather_column_rows};
use crate::context::Execution;
use crate::invalid;
use crate::join_key::JoinLayout;
use crate::join_storage::{HashIndex, combine_cell_hash, hash_values};
use crate::operators::DeferredInput;
use crate::pages::{BoxPageCursor, OwnedPage, PageCursor};
use crate::pushdown::{PushdownBuilder, Pushdowns};
use crate::runtime_schema::EmittedRelation;
use crate::scalar_key::ScalarKey;

/// For each equality, whether its first operand is the build side's key; `None` where the join needs the row join.
pub(crate) fn key_order(layout: &JoinLayout) -> Option<Vec<bool>> {
    if !layout.left.computes_no_keys() || !layout.right.computes_no_keys() || layout.comparisons.len() != layout.left.key_count() { return None; }
    layout.comparisons.iter().map(|comparison| match comparison {
        Expr::Binary { op: BinaryOp::Eq, left, right } => match (left.as_ref(), right.as_ref()) {
            (Expr::Column { id: first, data_type: first_type }, Expr::Column { data_type: second_type, .. })
                if rtdb_plan::validate_equality_types(first_type, second_type).is_ok() => Some(layout.left.original.get(*first).is_some()),
            _ => None,
        },
        _ => None,
    }).collect()
}

pub(crate) struct ProbeInput { pub runtime: Arc<EmittedRelation>, pub execution: Execution, pub pushdowns: Option<Arc<Pushdowns>> }

enum State {
    Build { build: BoxBatchCursor, probe: ProbeInput, layout: JoinLayout },
    Probe { table: Table, probe: BoxBatchCursor },
    Rows(BoxPageCursor),
    Done,
}

struct Table {
    batches: Vec<ColumnBatch>,
    starts: Vec<usize>,
    keys: BuildKeys,
    index: HashIndex,
    _keys: Arc<MemoryReservation>,
    _inventory: Arc<MemoryReservation>,
}

enum BuildKeys { Arrow, Values { keys: Vec<Value>, kind: Option<KeyKind> } }

impl Table {
    fn position(&self, ordinal: usize) -> (usize, usize) {
        let batch = self.starts.partition_point(|&start| start <= ordinal) - 1;
        (batch, ordinal - self.starts[batch])
    }
}

pub(crate) struct BatchJoin { state: State, spec: Spec }

/// What the join compares and how it reads cells, fixed for its lifetime.
/// `unswapped` is the join's own layout where the join builds its right input, which a spill hands to the row join.
struct Spec { order: Vec<bool>, build_keys: Vec<usize>, probe_keys: Vec<usize>, projected: bool, swapped: bool, unswapped: Option<JoinLayout>, validate_payloads: bool, execution: Execution }

impl BatchJoin {
    pub fn new(build: BoxBatchCursor, probe: ProbeInput, layout: JoinLayout, order: Vec<bool>, projected: bool, execution: Execution) -> Self {
        let build_keys = layout.left.key_positions().to_vec();
        let probe_keys = layout.right.key_positions().to_vec();
        Self { state: State::Build { build, probe, layout }, spec: Spec { order, build_keys, probe_keys, projected, swapped: false, unswapped: None, validate_payloads: false, execution } }
    }
}

impl BatchJoin {
    /// With the join's own layout, builds its right input and emits each match's left columns first, as the join's
    /// schema orders them.
    pub fn swapped(mut self, unswapped: Option<JoinLayout>) -> Self { self.spec.swapped = unswapped.is_some(); self.spec.unswapped = unswapped; self }

    pub fn validating_payloads(mut self, validate: bool) -> Self { self.spec.validate_payloads = validate; self }
}

impl Spec {
    fn cell(&self, column: &Column, row: usize) -> Result<Value> {
        let mut value = column.value(row)?;
        if self.projected { value.admit_expression_dates()?; }
        Ok(value)
    }

    async fn build(&mut self, mut build: BoxBatchCursor, probe: ProbeInput, layout: JoinLayout) -> Result<State> {
        let memory = Arc::clone(&self.execution.context.memory);
        let threshold = self.execution.context.options.join_build_rows;
        let mut batches = Vec::new();
        let mut rows = 0usize;
        let mut inventory = memory.reserve(256)?;
        while let Some(batch) = build.next_batch().await? {
            self.execution.context.check_cancelled()?;
            if self.validate_payloads { batch.validate_readable(&self.execution.context)?; }
            rows = rows.checked_add(batch.len).ok_or_else(size_error)?;
            let bytes = batches.len().checked_add(1).and_then(|count| count.checked_mul(4 * std::mem::size_of::<ColumnBatch>()))
                .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
            Arc::get_mut(&mut inventory).expect("The join owns its batch inventory").grow_to(bytes)?;
            batches.push(batch);
            if i64::try_from(rows).map_or(true, |rows| rows > threshold) { return self.spill(batches, build, probe, layout); }
        }
        let width = self.build_keys.len();
        let starts_bytes = batches.len().checked_mul(std::mem::size_of::<usize>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        let _hashes = memory.reserve(rows.checked_mul(std::mem::size_of::<u32>()).ok_or_else(size_error)?)?;
        let mut reservation = memory.reserve(starts_bytes)?;
        let mut hashes = Vec::with_capacity(rows);
        let mut starts = Vec::with_capacity(batches.len());
        let mut start = 0;
        for batch in &batches {
            starts.push(start);
            start += batch.len;
        }
        let keys = if self.arrow_build_hashes(&batches, &mut hashes)? {
            BuildKeys::Arrow
        } else {
            hashes.clear();
            let mut keys = Vec::new();
            let bytes = rows.checked_mul(width).and_then(|cells| cells.checked_mul(std::mem::size_of::<Value>()))
                .and_then(|bytes| bytes.checked_add(starts_bytes)).ok_or_else(size_error)?;
            let lease = Arc::get_mut(&mut reservation).expect("The join owns its build-key reservation");
            lease.grow_to(bytes)?;
            keys.reserve_exact(rows * width);
            for batch in &batches {
                for row in 0..batch.len {
                    self.execution.context.check_cancelled_at(row)?;
                    let first = keys.len();
                    let bound = key_bound(batch, row, &self.build_keys)?;
                    let retained = lease.bytes();
                    lease.grow_to(retained.checked_add(bound).ok_or_else(size_error)?)?;
                    for &position in &self.build_keys { keys.push(self.cell(&batch.columns[position], row)?); }
                    let actual = keys[first..].iter().try_fold(retained, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(size_error))?;
                    if actual <= lease.bytes() { lease.shrink_to(actual)?; } else { lease.grow_to(actual)?; }
                    hashes.push(hash_values(&keys[first..])?);
                }
            }
            let kind = if width == 1 { KeyKind::of_build(&keys) } else { None };
            BuildKeys::Values { keys, kind }
        };
        let index = HashIndex::new(&hashes, &memory)?;
        drop(hashes);
        drop(_hashes);
        let mut filters = PushdownBuilder::new(layout.condition.as_ref(), &layout.left.schema, &layout.right.original, i64::try_from(rows).map_err(|_| size_error())?, true, &self.execution)?;
        match &keys {
            BuildKeys::Arrow => for batch in &batches {
                for row in 0..batch.len {
                    self.execution.context.check_cancelled_at(row)?;
                    filters.insert_arrow(&layout.left.schema, batch, row, &memory)?;
                }
            },
            BuildKeys::Values { keys, .. } => {
                let _scratch = memory.reserve(layout.left.schema.len().checked_mul(std::mem::size_of::<Value>()).ok_or_else(size_error)?)?;
                let mut scratch = vec![Value::Null; layout.left.schema.len()];
                for row in 0..rows {
                    for (index, &position) in self.build_keys.iter().enumerate() { scratch[position] = keys[row * width + index].clone(); }
                    filters.insert(&layout.left.schema, &scratch, &self.execution).await?;
                }
            }
        }
        let filters = filters.finish(&memory)?;
        let pushdowns = Pushdowns::merge(probe.pushdowns.as_ref(), filters, &memory)?;
        let Some((probe_batches, probe_projected)) = crate::operators::build_batches(probe.runtime, probe.execution, pushdowns).await? else {
            return Err(rtdb_types::Anomaly::fault("rtdb.query/batch-join", "A batch join's probe input did not open as batches"));
        };
        if probe_projected != self.projected { return Err(rtdb_types::Anomaly::fault("rtdb.query/batch-join", "A batch join's inputs disagree on date admission")); }
        Ok(State::Probe { table: Table { batches, starts, keys, index, _keys: reservation, _inventory: inventory }, probe: probe_batches })
    }

    fn arrow_build_hashes(&self, batches: &[ColumnBatch], hashes: &mut Vec<u32>) -> Result<bool> {
        for batch in batches {
            for row in 0..batch.len {
                self.execution.context.check_cancelled_at(row)?;
                let Some(hash) = arrow_hash(batch, row, &self.build_keys) else { return Ok(false); };
                hashes.push(hash);
            }
        }
        Ok(true)
    }

    /// Hands the input read so far, and the rest, to the row join, which spills a build side this large.
    fn spill(&mut self, held: Vec<ColumnBatch>, rest: BoxBatchCursor, probe: ProbeInput, layout: JoinLayout) -> Result<State> {
        let memory = &self.execution.context.memory;
        let pages = held.iter().map(|batch| batch.rows_admitting(memory, self.projected)).collect::<Result<VecDeque<_>>>()?;
        drop(held);
        let rest: BoxPageCursor = Box::new(crate::columnar::BatchRows::new(rest, Arc::clone(memory)).admitting_dates(self.projected));
        let build: BoxPageCursor = Box::new(HeldThen { pages, rest });
        let probe = DeferredInput::new(probe.runtime, probe.execution, crate::inspection::Constraints::default(), probe.pushdowns);
        let (layout, build_left) = match self.unswapped.take() { Some(unswapped) => (unswapped, false), None => (layout, true) };
        Ok(State::Rows(Box::new(crate::join::JoinCursor::new(build, probe, layout, build_left, rtdb_plan::JoinKind::Inner, self.execution.clone())?)))
    }

    fn probe_batch(&self, table: &Table, probe: &ColumnBatch) -> Result<Option<ColumnBatch>> {
        if self.validate_payloads { probe.validate_readable(&self.execution.context)?; }
        let mut matched = Matches { pairs: Vec::new(), probes: Vec::new(), charge: self.execution.context.memory.reserve(4096)? };
        match &table.keys {
            BuildKeys::Arrow => self.probe_arrow_keys(table, probe, &mut matched)?,
            BuildKeys::Values { keys, kind } => self.probe_value_keys(table, probe, keys, *kind, &mut matched)?,
        }
        let Matches { pairs, probes, charge: matches } = matched;
        if pairs.is_empty() { return Ok(None); }
        let memory = &self.execution.context.memory;
        let build_width = table.batches.first().map_or(0, |batch| batch.columns.len());
        let width = build_width.checked_add(probe.columns.len()).ok_or_else(size_error)?;
        let bound = width.checked_mul(std::mem::size_of::<Column>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        let mut reservation = memory.reserve(bound)?;
        let mut columns = Vec::with_capacity(width);
        let mut append = |batches: &[ColumnBatch], rows: &[(usize, usize)], width| -> Result<()> {
            for position in 0..width {
                let (column, _scratch) = gather_column_rows(|batch| &batches[batch], rows, position, memory)?;
                let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("join-size", "A join output charge is shared"))?;
                lease.grow_to(lease.bytes().checked_add(column.retained_bytes()?).ok_or_else(size_error)?)?;
                columns.push(column);
            }
            Ok(())
        };
        if self.swapped { append(std::slice::from_ref(probe), &probes, probe.columns.len())?; }
        append(&table.batches, &pairs, build_width)?;
        if !self.swapped { append(std::slice::from_ref(probe), &probes, probe.columns.len())?; }
        drop(matches);
        ColumnBatch::settled(columns, pairs.len(), reservation).map(Some)
    }

    fn probe_value_keys(&self, table: &Table, probe: &ColumnBatch, keys: &[Value], kind: Option<KeyKind>, matches: &mut Matches) -> Result<()> {
        if let Some(key) = kind.and_then(|kind| ProbeKey::of(&probe.columns[self.probe_keys[0]], kind)) {
            for row in 0..probe.len {
                self.execution.context.check_cancelled_at(row)?;
                let Some(hash) = key.hash(row) else { continue };
                for (candidate, build) in table.index.candidates(hash).enumerate() {
                    self.execution.context.check_cancelled_at(candidate)?;
                    if key.equals(row, &keys[build]) { matches.push(table, build, row)?; }
                }
            }
            return Ok(());
        }
        let width = self.build_keys.len();
        let equality = self.execution.scope()?;
        for row in 0..probe.len {
            self.execution.context.check_cancelled()?;
            let (key, _scratch) = self.value_keys(probe, row, &self.probe_keys)?;
            for (candidate, build) in table.index.candidates(hash_values(&key)?).enumerate() {
                self.execution.context.check_cancelled_at(candidate)?;
                if self.equal_values(&keys[build * width..(build + 1) * width], &key, &equality)? {
                    matches.push(table, build, row)?;
                }
            }
        }
        Ok(())
    }

    fn probe_arrow_keys(&self, table: &Table, probe: &ColumnBatch, matches: &mut Matches) -> Result<()> {
        for row in 0..probe.len {
            self.execution.context.check_cancelled_at(row)?;
            let mut probe_values = None;
            let hash = match arrow_hash(probe, row, &self.probe_keys) {
                Some(hash) => hash,
                None => {
                    let key = self.value_keys(probe, row, &self.probe_keys)?;
                    let hash = hash_values(&key.0)?;
                    probe_values = Some(key);
                    hash
                }
            };
            for (candidate, build) in table.index.candidates(hash).enumerate() {
                self.execution.context.check_cancelled_at(candidate)?;
                let (batch_index, build_row) = table.position(build);
                let batch = &table.batches[batch_index];
                let typed = if probe_values.is_none() { self.equal_arrow(batch, build_row, probe, row) } else { None };
                let matched = match typed {
                    Some(matched) => matched,
                    None => {
                        if probe_values.is_none() {
                            probe_values = Some(self.value_keys(probe, row, &self.probe_keys)?);
                        }
                        let (probe_key, _) = probe_values.as_ref().expect("A fallback probe key has been read");
                        let (build_key, _scratch) = self.value_keys(batch, build_row, &self.build_keys)?;
                        self.equal_values(&build_key, probe_key, &self.execution.scope()?)?
                    }
                };
                if matched { matches.push(table, build, row)?; }
            }
        }
        Ok(())
    }

    fn equal_arrow(&self, build: &ColumnBatch, build_row: usize, probe: &ColumnBatch, probe_row: usize) -> Option<bool> {
        let mut matched = true;
        for (&left, &right) in self.build_keys.iter().zip(&self.probe_keys) {
            let left = ScalarKey::read(&build.columns[left], build_row)?;
            let right = ScalarKey::read(&probe.columns[right], probe_row)?;
            matched &= left.equal(right)? && left != ScalarKey::Null;
        }
        Some(matched)
    }

    fn equal_values(&self, build: &[Value], probe: &[Value], equality: &rtdb_expr::EvaluationScope) -> Result<bool> {
        let mut unknown = false;
        for ((build, probe), build_first) in build.iter().zip(probe).zip(&self.order) {
            let (left, right) = if *build_first { (build, probe) } else { (probe, build) };
            let equal = rtdb_expr::sql_eq_in_timezone(left, right, &self.execution.context.timezone, equality)?;
            equality.reset()?;
            match equal {
                None => unknown = true,
                Some(false) => return Ok(false),
                Some(true) => {}
            }
        }
        Ok(!unknown)
    }

    fn value_keys(&self, batch: &ColumnBatch, row: usize, positions: &[usize]) -> Result<(Vec<Value>, Arc<MemoryReservation>)> {
        let reservation = self.execution.context.memory.reserve(key_bound(batch, row, positions)?)?;
        let keys = positions.iter().map(|&position| self.cell(&batch.columns[position], row)).collect::<Result<_>>()?;
        Ok((keys, reservation))
    }
}

fn arrow_hash(batch: &ColumnBatch, row: usize, positions: &[usize]) -> Option<u32> {
    positions.iter().try_fold(0, |hash, &position| {
        ScalarKey::read(&batch.columns[position], row).map(|key| combine_cell_hash(hash, key.hash() as i32))
    })
}

fn key_bound(batch: &ColumnBatch, row: usize, positions: &[usize]) -> Result<usize> {
    let base = positions.len().checked_mul(std::mem::size_of::<Value>())
        .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
    positions.iter().try_fold(base, |bytes, &position| {
        batch.columns[position].bound(row)?.checked_mul(2).and_then(|bound| bytes.checked_add(bound)).ok_or_else(size_error)
    })
}

/// The build and probe rows a probe batch matched, in probe order, charged as they grow.
struct Matches { pairs: Vec<(usize, usize)>, probes: Vec<(usize, usize)>, charge: Arc<MemoryReservation> }
impl Matches {
    fn push(&mut self, table: &Table, build: usize, row: usize) -> Result<()> {
        if self.pairs.len() == self.pairs.capacity() {
            let capacity = self.pairs.capacity().saturating_mul(2).max(1024);
            let lease = Arc::get_mut(&mut self.charge).ok_or_else(|| invalid("join-size", "A join match charge is shared"))?;
            lease.grow_to(capacity.checked_mul(2 * std::mem::size_of::<(usize, usize)>()).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?)?;
            self.pairs.reserve_exact(capacity - self.pairs.len());
            self.probes.reserve_exact(capacity - self.probes.len());
        }
        self.pairs.push(table.position(build));
        self.probes.push((0, row));
        Ok(())
    }
}

/// A single join key whose build cells are all integers of any width, or all strings: SQL equality of two such cells
/// is equality of the integer or of the bytes, and a null matches nothing, so the probe reads its cells straight
/// from its arrays rather than as values.
#[derive(Clone, Copy, PartialEq, Eq)]
enum KeyKind { Int, Utf8 }
impl KeyKind {
    fn of_build(keys: &[Value]) -> Option<Self> {
        let int = |value: &Value| matches!(value, Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_));
        if keys.iter().all(|key| int(key) || matches!(key, Value::Null)) { return Some(Self::Int); }
        if keys.iter().all(|key| matches!(key, Value::Utf8(_) | Value::Null)) { return Some(Self::Utf8); }
        None
    }
}
fn int_value(value: &Value) -> Option<i64> {
    match value { Value::I8(v) => Some(i64::from(*v)), Value::I16(v) => Some(i64::from(*v)), Value::I32(v) => Some(i64::from(*v)), Value::I64(v) => Some(*v), _ => None }
}

/// A probe batch's key cells, where its column holds only the build key's kind (an integer union's legs included).
enum ProbeKey<'a> { Int(Vec<Option<i64>>), Utf8(&'a arrow_array::StringArray) }
impl<'a> ProbeKey<'a> {
    fn of(column: &'a Column, kind: KeyKind) -> Option<Self> {
        match (column, kind) {
            (Column::Arrow { array, field, .. }, KeyKind::Utf8) if field.metadata().is_empty() => array.as_any().downcast_ref::<arrow_array::StringArray>().map(Self::Utf8),
            (Column::Arrow { array, field, .. }, KeyKind::Int) if field.metadata().is_empty() => int_cells(array.as_ref()).map(Self::Int),
            (Column::Values(values), KeyKind::Int) => values.iter().map(|value| match value { Value::Null => Some(None), value => int_value(value).map(Some) }).collect::<Option<Vec<_>>>().map(Self::Int),
            _ => None,
        }
    }
    /// The row's hash as `hash_values` takes it of the cell's value; none for a null, which matches nothing.
    fn hash(&self, row: usize) -> Option<u32> {
        let cell = match self {
            Self::Int(cells) => rtdb_columnar::hash_value(&Value::I64(cells[row]?), 0).ok()?,
            Self::Utf8(array) => { if array.is_null(row) { return None; } rtdb_columnar::xxhash32(array.value(row).as_bytes(), 0) as i32 }
        };
        Some(combine_cell_hash(0, cell))
    }
    fn equals(&self, row: usize, build: &Value) -> bool {
        match (self, build) {
            (Self::Int(cells), build) => cells[row].is_some_and(|probe| int_value(build) == Some(probe)),
            (Self::Utf8(array), Value::Utf8(build)) => !array.is_null(row) && array.value(row) == build.as_str(),
            _ => false,
        }
    }
}
/// An integer column's cells widened to i64, through each leg of a dense union whose legs are integers or null.
pub(crate) fn int_cells(array: &dyn Array) -> Option<Vec<Option<i64>>> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int8Type, Int16Type, Int32Type, Int64Type};
    use arrow_schema::DataType;
    let scalar = |array: &dyn Array, row: usize| -> Option<Option<i64>> {
        if array.is_null(row) { return Some(None); }
        Some(Some(match array.data_type() {
            DataType::Int8 => i64::from(array.as_primitive::<Int8Type>().value(row)),
            DataType::Int16 => i64::from(array.as_primitive::<Int16Type>().value(row)),
            DataType::Int32 => i64::from(array.as_primitive::<Int32Type>().value(row)),
            DataType::Int64 => array.as_primitive::<Int64Type>().value(row),
            DataType::Null => return Some(None),
            _ => return None,
        }))
    };
    match array.data_type() {
        DataType::Union(legs, arrow_schema::UnionMode::Dense) => {
            if !legs.iter().all(|(_, leg)| leg.metadata().is_empty() && matches!(leg.data_type(), DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Null)) { return None; }
            let union = array.as_any().downcast_ref::<arrow_array::UnionArray>()?;
            (0..union.len()).map(|row| scalar(union.child(union.type_id(row)).as_ref(), union.value_offset(row))).collect()
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => (0..array.len()).map(|row| scalar(array, row)).collect(),
        _ => None,
    }
}

#[async_trait::async_trait]
impl BatchCursor for BatchJoin {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        loop {
            self.spec.execution.context.check_cancelled()?;
            match std::mem::replace(&mut self.state, State::Done) {
                State::Build { build, probe, layout } => { self.state = self.spec.build(build, probe, layout).await?; }
                State::Probe { table, mut probe } => {
                    let Some(batch) = probe.next_batch().await? else { return Ok(None) };
                    let output = self.spec.probe_batch(&table, &batch)?;
                    self.state = State::Probe { table, probe };
                    if let Some(output) = output { return Ok(Some(output)); }
                }
                State::Rows(mut rows) => {
                    let Some(page) = rows.next_page().await? else { return Ok(None) };
                    let batch = ColumnBatch::from_page(&page, &self.spec.execution.context.memory)?;
                    self.state = State::Rows(rows);
                    if batch.len > 0 { return Ok(Some(batch)); }
                }
                State::Done => return Ok(None),
            }
        }
    }
}

/// Pages already read from the build input, then the rest of it.
struct HeldThen { pages: VecDeque<OwnedPage>, rest: BoxPageCursor }
#[async_trait::async_trait]
impl PageCursor for HeldThen {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        if let Some(page) = self.pages.pop_front() { return Ok(Some(page)); }
        self.rest.next_page().await
    }
}

fn size_error() -> rtdb_types::Anomaly { invalid("join-size", "Join output exceeds addressable memory") }

