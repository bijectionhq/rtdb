//! Column-at-a-time pages: a scan gathers the cells its visible rows resolve to into one column per output field,
//! so filters and aggregates can read whole columns, and rows are materialised only for operators that need them.
mod patch;
pub(crate) mod search;

use std::{collections::{HashMap, HashSet}, ops::Range, sync::Arc};

use arrow_array::{Array, ArrayRef, FixedSizeListArray, ListArray, MapArray, StructArray, UnionArray};
use arrow_schema::{DataType, Field as ArrowField};
use rtdb_columnar::{MemoryBudget, MemoryReservation, copy_selected_cells, read_execution_value, read_execution_value_as};
use rtdb_index::IndexBatch;
use rtdb_plan::Field;
use rtdb_temporal::ResolvedRow;
use rtdb_types::{ListKind, LogicalType, Result, Timestamp, Value};

use crate::{cursor::OwnedRow, invalid, pages::{OwnedPage, PageBuilder}};

/// One output field of a page.
pub(crate) enum Column {
    /// Cells read through the source field and the put child's logical type, exactly as a row read takes them.
    Arrow { array: ArrayRef, field: Arc<ArrowField>, logical: Option<LogicalType> },
    Selected(SelectedColumn),
    /// Cells materialised by an operator.
    Values(Vec<Value>),
}

pub(crate) type ArrowVector = (ArrayRef, Arc<ArrowField>, Option<LogicalType>);
type StoredVector = (ArrayRef, Arc<ArrowField>, Option<LogicalType>, Option<Arc<MemoryReservation>>);

#[derive(Clone)]
pub(crate) struct SelectedColumn {
    sources: Arc<Vec<Option<StoredVector>>>,
    cells: Arc<Vec<(usize, usize)>>,
}

impl SelectedColumn {
    pub(crate) fn new(sources: Vec<Option<StoredVector>>, cells: Arc<Vec<(usize, usize)>>) -> Self {
        Self { sources: Arc::new(sources), cells }
    }

    fn source(&self, row: usize) -> Result<Option<(&StoredVector, usize)>> {
        let &(source, offset) = self.cells.get(row).ok_or_else(|| invalid("column-row", "Column row is outside its page"))?;
        Ok(self.sources[source].as_ref().map(|source| (source, offset)))
    }

    pub(crate) fn source_layouts(&self) -> impl Iterator<Item = (&dyn Array, &ArrowField, Option<&LogicalType>)> {
        self.sources.iter().flatten().map(|(array, field, logical, _)| (array.as_ref(), field.as_ref(), logical.as_ref()))
    }

    fn select(self, mask: &[bool], selected: usize) -> Self {
        let mut cells = Vec::with_capacity(selected);
        cells.extend(self.cells.iter().zip(mask).filter_map(|(&cell, &keep)| keep.then_some(cell)));
        Self { sources: self.sources, cells: Arc::new(cells) }
    }

    pub(crate) fn fragment_boundaries(&self, boundaries: &mut [bool]) {
        let layout = |source: usize| self.sources[source].as_ref().map(|(_, field, logical, _)| (field, logical));
        for (boundary, cells) in boundaries.iter_mut().skip(1).zip(self.cells.windows(2)) {
            let (left, right) = (cells[0].0, cells[1].0);
            if !*boundary && left != right && layout(left) != layout(right) {
                *boundary = true;
            }
        }
    }

    fn list_values(&self, name: &str, memory: &Arc<MemoryBudget>) -> Result<(ArrayRef, Arc<ArrowField>, Arc<MemoryReservation>)> {
        use arrow_buffer::ScalarBuffer;
        use arrow_schema::UnionFields;

        type Leg = (ArrayRef, Arc<ArrowField>, Arc<MemoryReservation>);

        fn union(name: &str, mut legs: Vec<Leg>, memory: &Arc<MemoryBudget>) -> Result<Leg> {
            if legs.len() == 1 { return Ok(legs.into_iter().next().unwrap()); }
            let rows = legs.iter().try_fold(0usize, |rows, (array, _, _)| rows.checked_add(array.len()).ok_or_else(size_error))?;
            let metadata = legs.iter().try_fold(rows.checked_mul(16).ok_or_else(size_error)?, |bytes, (_, field, _)| {
                bytes.checked_add(field.size().checked_mul(4).ok_or_else(size_error)?).ok_or_else(size_error)
            })?;
            let mut reservation = memory.reserve(metadata.checked_add(4096).ok_or_else(size_error)?)?;
            let mut tags = Vec::with_capacity(rows);
            let mut offsets = Vec::with_capacity(rows);
            let mut fields = Vec::with_capacity(legs.len());
            let mut children = Vec::with_capacity(legs.len());
            for (tag, (array, field, _)) in legs.iter().enumerate() {
                tags.extend(std::iter::repeat_n(tag as i8, array.len()));
                offsets.extend(0..i32::try_from(array.len()).map_err(|_| size_error())?);
                fields.push(Arc::new(field.as_ref().clone().with_name(format!("layout-{tag}"))));
                children.push(Arc::clone(array));
            }
            let fields = UnionFields::try_new((0..fields.len()).map(|tag| tag as i8), fields)
                .map_err(|error| invalid("column-list", error))?;
            let array: ArrayRef = Arc::new(UnionArray::try_new(fields, ScalarBuffer::from(tags), Some(ScalarBuffer::from(offsets)), children)
                .map_err(|error| invalid("column-list", error))?);
            let field = Arc::new(ArrowField::new(name, array.data_type().clone(), array.is_nullable()));
            let retained = array.get_array_memory_size().checked_add(field.size()).ok_or_else(size_error)?;
            let lease = Arc::get_mut(&mut reservation).expect("A list union owns its reservation");
            for (_, _, charge) in &mut legs {
                lease.absorb(Arc::get_mut(charge).expect("A list union owns each fragment reservation"))?;
            }
            if retained > lease.bytes() { lease.grow_to(retained)?; } else { lease.shrink_to(retained)?; }
            Ok((array, field, reservation))
        }

        let _inventory = memory.reserve(self.cells.len().checked_mul(96).ok_or_else(size_error)?)?;
        let mut boundaries = vec![false; self.cells.len()];
        self.fragment_boundaries(&mut boundaries);
        let ends = boundaries.into_iter().enumerate().filter_map(|(index, boundary)| boundary.then_some(index))
            .chain(std::iter::once(self.cells.len()));
        let mut legs = Vec::new();
        let mut start = 0;
        for end in ends {
            let ((array, field, _), charge) = self.fragment_vector(start..end, memory)?;
            let logical = rtdb_columnar::from_field(&field)?;
            let unencoded = logical.unencoded();
            let leg = if logical != unencoded {
                let target = rtdb_columnar::to_field(field.name(), &unencoded)?;
                let bound = rtdb_columnar::arrow_relayout_bound(array.as_ref(), &field, &target)?;
                let _relayout = memory.reserve(bound)?;
                let (field, array) = rtdb_columnar::relayout_arrow_column(array, &field, &target)?;
                let retained = memory.reserve(array.get_array_memory_size().checked_add(field.size()).ok_or_else(size_error)?)?;
                (array, Arc::new(field), retained)
            } else {
                (array, field, charge)
            };
            legs.push(leg);
            start = end;
        }
        const FAN_OUT: usize = 128;
        while legs.len() > FAN_OUT {
            let mut children = legs.into_iter();
            legs = Vec::new();
            while children.len() != 0 {
                legs.push(union(name, children.by_ref().take(FAN_OUT).collect(), memory)?);
            }
        }
        union(name, legs, memory)
    }

    pub(crate) fn fragment_vector(&self, rows: Range<usize>, memory: &Arc<MemoryBudget>)
        -> Result<(ArrowVector, Arc<MemoryReservation>)> {
        let layout = |source: usize| self.sources[source].as_ref().map(|(_, field, logical, _)| (field, logical));
        let cells = &self.cells[rows.clone()];
        if let Some(&(first, _)) = cells.first()
            && cells.iter().any(|&(source, _)| layout(source) != layout(first)) {
            return Err(invalid("fragment-layout", "Fragment rows differ in layout"));
        }
        let first = cells.first().and_then(|&(source, _)| self.sources[source].as_ref());
        let Some((_, field, logical, _)) = first else {
            let name = self.sources.iter().flatten().next().map_or("", |(_, field, ..)| field.name());
            let array = Arc::new(arrow_array::NullArray::new(rows.len())) as ArrayRef;
            let field = Arc::new(ArrowField::new(name, DataType::Null, true));
            let reservation = memory.reserve(array.get_array_memory_size())?;
            return Ok(((array, field, Some(LogicalType::Null)), reservation));
        };

        let sources = self.sources.len().min(cells.len());
        let base = sources.checked_mul(64)
            .and_then(|bytes| field.size().checked_mul(8).and_then(|schema| bytes.checked_add(schema)))
            .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
        let bound = cells.iter().try_fold(base, |bytes, &(source, row)| {
            let (array, field, ..) = self.sources[source].as_ref().ok_or_else(|| invalid("fragment-layout", "Fragment rows differ in layout"))?;
            rtdb_columnar::estimate_copy_cell_bytes(field, array.as_ref(), row)?.checked_mul(4)
                .and_then(|size| bytes.checked_add(size)).ok_or_else(size_error)
        })?;
        let mut reservation = memory.reserve(bound)?;
        let mut inputs = Vec::with_capacity(sources);
        let mut positions = HashMap::with_capacity(sources);
        let mut picks = Vec::with_capacity(cells.len());
        for &(source, row) in cells {
            let position = *positions.entry(source).or_insert_with(|| {
                let (array, ..) = self.sources[source].as_ref().expect("The selection was checked");
                let position = inputs.len();
                inputs.push(array.as_ref());
                position
            });
            picks.push((position, row));
        }
        let output = copy_selected_cells(&inputs, &picks)?;
        let lease = Arc::get_mut(&mut reservation).expect("A fragment vector owns its reservation");
        lease.shrink_to(output.get_array_memory_size())?;
        Ok(((output, Arc::clone(field), logical.clone()), reservation))
    }

    fn null_test(&self, is_null: bool) -> Option<Vec<Option<bool>>> {
        if !self.sources.iter().flatten().all(|(_, field, logical, _)| logical.as_ref().is_some_and(|ty| stable_read_layout(ty, field))) {
            return None;
        }
        self.cells.iter().map(|&(source, row)| {
            let null = match &self.sources[source] {
                Some((array, field, ..)) => arrow_cell_is_null(field, array.as_ref(), row)?,
                None => true,
            };
            Some(Some(null == is_null))
        }).collect()
    }
}

fn arrow_cell_is_null(field: &ArrowField, array: &dyn Array, row: usize) -> Option<bool> {
    match field.data_type() {
        DataType::Null => field.is_nullable().then_some(true),
        DataType::Union(fields, arrow_schema::UnionMode::Dense) => {
            let union = array.as_any().downcast_ref::<UnionArray>()?;
            let id = union.type_id(row);
            let child = fields.iter().find_map(|(tag, field)| (tag == id).then_some(field))?;
            arrow_cell_is_null(child, union.child(id).as_ref(), union.value_offset(row))
        }
        _ => Some(array.is_null(row)),
    }
}

fn arrow_value(array: &dyn Array, field: &ArrowField, logical: Option<&LogicalType>, row: usize) -> Result<Value> {
    let value = match logical {
        Some(logical) => read_execution_value_as(logical, field, array, row)?,
        None => read_execution_value(field, array, row)?,
    };
    Ok(if matches!(value, Value::Undefined) { Value::Null } else { value })
}

fn arrow_value_bound(array: &dyn Array, field: &ArrowField, logical: Option<&LogicalType>, row: usize) -> Result<usize> {
    if logical.is_some() { rtdb_columnar::estimate_read_cell_bytes(field, array, row) }
    else { rtdb_columnar::estimate_read_value_bytes(field, array, row) }
}

impl Column {
    pub(crate) fn arrow_cell(&self, row: usize) -> Option<Option<(&dyn Array, &ArrowField, Option<&LogicalType>, usize)>> {
        match self {
            Self::Arrow { array, field, logical } => Some(Some((array.as_ref(), field, logical.as_ref(), row))),
            Self::Selected(column) => Some(column.source(row).ok()?.map(|((array, field, logical, _), offset)| {
                (array.as_ref(), field.as_ref(), logical.as_ref(), offset)
            })),
            Self::Values(_) => None,
        }
    }

    pub(crate) fn validate_readable_cell(&self, row: usize, memory: &Arc<MemoryBudget>) -> Result<()> {
        if let Some(Some((array, field, logical, offset))) = self.arrow_cell(row) {
            let derived;
            let _schema;
            let logical = match logical {
                Some(logical) => logical,
                None => {
                    _schema = memory.reserve(field.size().checked_mul(8).ok_or_else(size_error)?)?;
                    derived = rtdb_columnar::from_field(field)?;
                    &derived
                }
            };
            if logical == &LogicalType::Nothing {
                return Err(rtdb_types::Anomaly::fault("xtdb/zero-leg-read", "The bottom type has no values to read"));
            }
            rtdb_columnar::validate_readable_cell(field, array, logical, offset, memory)?;
        }
        Ok(())
    }

    pub(crate) fn null_at(&self, row: usize) -> Option<bool> {
        match self {
            Self::Values(values) => values.get(row).map(|value| matches!(value, Value::Null | Value::Undefined)),
            column => match column.arrow_cell(row)? {
                Some((array, field, Some(logical), offset)) if stable_read_layout(logical, field) => arrow_cell_is_null(field, array, offset),
                None => Some(true),
                _ => None,
            },
        }
    }

    pub fn value(&self, row: usize) -> Result<Value> {
        match self {
            Self::Arrow { array, field, logical } => arrow_value(array.as_ref(), field, logical.as_ref(), row),
            Self::Selected(column) => column.source(row)?.map_or(Ok(Value::Null), |((array, field, logical, _), offset)| {
                arrow_value(array.as_ref(), field, logical.as_ref(), offset)
            }),
            Self::Values(values) => values.get(row).cloned().ok_or_else(|| invalid("column-row", "Column row is outside its page")),
        }
    }
    pub fn bound(&self, row: usize) -> Result<usize> {
        match self {
            Self::Arrow { array, field, logical } => arrow_value_bound(array.as_ref(), field, logical.as_ref(), row),
            Self::Selected(column) => column.source(row)?.map_or(Ok(std::mem::size_of::<Value>()), |((array, field, logical, _), offset)| {
                arrow_value_bound(array.as_ref(), field, logical.as_ref(), offset)
            }),
            Self::Values(values) => values.get(row).map_or(Ok(0), |value| value.retained_bytes().map(|bytes| bytes.saturating_add(std::mem::size_of::<Value>()))),
        }
    }
    pub(crate) fn retained_bytes(&self) -> Result<usize> {
        match self {
            Self::Arrow { array, .. } => Ok(array.get_array_memory_size()),
            Self::Selected(column) => {
                let cells = column.cells.capacity().checked_mul(std::mem::size_of::<(usize, usize)>()).ok_or_else(size_error)?;
                let sources = column.sources.capacity().checked_mul(std::mem::size_of::<Option<StoredVector>>()).ok_or_else(size_error)?;
                let base = cells.checked_add(sources).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
                column.sources.iter().flatten().try_fold(base, |bytes, (array, _, logical, owner)| {
                    let owned = if owner.is_none() { array.get_array_memory_size() } else { 0 };
                    let metadata = logical.as_ref().map_or(Ok(0), LogicalType::retained_bytes)?;
                    bytes.checked_add(owned).and_then(|bytes| bytes.checked_add(metadata)).ok_or_else(size_error)
                })
            }
            Self::Values(values) => values.iter().try_fold(values.capacity().saturating_mul(std::mem::size_of::<Value>()), |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(size_error)),
        }
    }
}

/// A page of columns sharing one row count, charged for every column it retains.
pub(crate) struct ColumnBatch {
    pub columns: Vec<Column>,
    pub len: usize,
    reservation: Arc<MemoryReservation>,
    appended: Vec<Arc<MemoryReservation>>,
}

impl ColumnBatch {
    pub(crate) fn supports_literal_rows(rows: &[Vec<rtdb_plan::Expr>]) -> bool {
        let Some(first) = rows.first() else { return true; };
        rows.iter().all(|row| row.len() == first.len() && row.iter().all(|expression| literal_cell(expression).is_some()))
            && (0..first.len()).all(|position| scalar_result_type(rows.iter().map(|row| {
                literal_cell(&row[position]).expect("The row contains only literal cells")
            })).is_some())
    }

    pub(crate) fn from_literal_rows(rows: &[Vec<rtdb_plan::Expr>], context: &crate::QueryContext) -> Result<Option<Self>> {
        let Some(first) = rows.first() else { return Ok(None); };
        let memory = &context.memory;
        let mut batch = Self::settled(vec![], rows.len(), memory.reserve(256)?)?;
        for position in 0..first.len() {
            context.check_cancelled()?;
            let cells = rows.iter().map(|row| literal_cell(&row[position]).expect("The row contains only literal cells"));
            let bytes = rows.len().checked_mul(std::mem::size_of::<Value>()).ok_or_else(size_error)?;
            let bytes = cells.clone().try_fold(bytes, |bytes, value| {
                bytes.checked_add(value.retained_bytes()?).ok_or_else(size_error)
            })?;
            let scratch = memory.reserve(bytes)?;
            let values = cells.cloned().collect::<Vec<_>>();
            let ty = scalar_result_type(values.iter()).ok_or_else(|| invalid("column-literal", "An admitted literal column has mixed types"))?;
            let (column, charge) = pack_scalar_values(&values, ty, memory)?;
            drop((values, scratch));
            batch.append_column(column, charge, memory)?;
        }
        Ok(Some(batch))
    }

    pub(crate) fn cross(left: &Self, right: &Self, rows: usize, context: &crate::QueryContext) -> Result<Self> {
        let memory = &context.memory;
        let width = left.columns.len().checked_add(right.columns.len()).ok_or_else(size_error)?;
        let base = width.checked_mul(256).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
        let mut reservation = memory.reserve(base)?;
        if rows == 0 {
            let columns = (0..width).map(|_| Column::Values(Vec::new())).collect();
            return Self::settled(columns, 0, reservation);
        }
        let positions = rows.checked_mul(2 * std::mem::size_of::<(usize, usize)>()).ok_or_else(size_error)?;
        let _picks = memory.reserve(positions)?;
        let mut left_rows = Vec::with_capacity(rows);
        let mut right_rows = Vec::with_capacity(rows);
        for left_row in 0..left.len {
            context.check_cancelled()?;
            for right_row in 0..right.len {
                context.check_cancelled_at(right_row)?;
                left_rows.push((0, left_row));
                right_rows.push((0, right_row));
            }
        }
        let mut columns = Vec::with_capacity(width);
        for (batch, positions) in [(left, left_rows), (right, right_rows)] {
            for index in 0..batch.columns.len() {
                let (column, _scratch) = gather_column_rows(|_| batch, &positions, index, memory)?;
                let lease = Arc::get_mut(&mut reservation).expect("Cross join owns its output reservation");
                lease.grow_to(lease.bytes().checked_add(column.retained_bytes()?).ok_or_else(size_error)?)?;
                columns.push(column);
            }
        }
        Self::settled(columns, rows, reservation)
    }

    pub(crate) fn expand_list(&self, position: usize, ordinality: Option<&str>, outer: bool, context: &crate::QueryContext)
        -> Result<Option<Self>> {
        let memory = &context.memory;
        let base = self.len.checked_mul(512).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
        let mut selection = memory.reserve(base)?;
        let mut inputs = HashMap::new();
        let mut layouts = HashSet::new();
        let mut sources = vec![None];
        let mut spans = Vec::with_capacity(self.len);
        let mut rows = 0usize;
        let list = &self.columns[position];
        for row in 0..self.len {
            context.check_cancelled_at(row)?;
            let vector = match list {
                Column::Arrow { array, field, logical } => Some((array, field, logical.as_ref(), row, Some(self.column_reservation(position)))),
                Column::Selected(selected) => selected.source(row)?.map(|((array, field, logical, custody), offset)| {
                    (array, field, logical.as_ref(), offset, custody.as_ref())
                }),
                Column::Values(_) => return Ok(None),
            };
            let (source, start, count) = match vector {
                None => (0, 0, 0),
                Some((array, field, logical, offset, custody)) => {
                    let Some(logical) = logical else { return Ok(None); };
                    let layout = (Arc::as_ptr(field), std::ptr::from_ref(logical));
                    if layouts.insert(layout) {
                        let lease = Arc::get_mut(&mut selection).expect("Unnest owns the selection reservation");
                        let bytes = field.size().checked_mul(8).ok_or_else(size_error)?;
                        lease.grow_to(lease.bytes().checked_add(bytes).ok_or_else(size_error)?)?;
                        if !stable_read_layout(logical, field) || !readable_list_type(logical, field) {
                            return Ok(None);
                        }
                    }
                    if array.is_null(offset) || *logical == LogicalType::Null {
                        (0, 0, 0)
                    } else {
                        let (field, values, start, count) = match array.data_type() {
                            DataType::List(field) => {
                                use arrow_array::cast::AsArray;
                                let list = array.as_list::<i32>();
                                let offsets = list.value_offsets();
                                let start = offsets[offset] as usize;
                                (field, list.values(), start, offsets[offset + 1] as usize - start)
                            }
                            DataType::FixedSizeList(field, width) => {
                                use arrow_array::cast::AsArray;
                                let width = *width as usize;
                                let list = array.as_fixed_size_list();
                                (field, list.values(), offset * width, width)
                            }
                            _ => return Ok(None),
                        };
                        let key = (Arc::as_ptr(values).cast::<()>(), Arc::as_ptr(field));
                        let source = if let Some(&source) = inputs.get(&key) {
                            source
                        } else {
                            let lease = Arc::get_mut(&mut selection).expect("Unnest owns the selection reservation");
                            let bytes = field.size().checked_mul(8).ok_or_else(size_error)?;
                            lease.grow_to(lease.bytes().checked_add(bytes).ok_or_else(size_error)?)?;
                            let source = sources.len();
                            sources.push(Some((Arc::clone(values), Arc::clone(field), Some(rtdb_columnar::from_field(field)?), custody.cloned())));
                            inputs.insert(key, source);
                            source
                        };
                        (source, start, count)
                    }
                }
            };
            if ordinality.is_some() && i32::try_from(count).is_err() {
                return Err(invalid("ordinality-overflow", "List ordinal exceeds signed32 bits"));
            }
            rows = rows.checked_add(if outer { count.max(1) } else { count }).ok_or_else(size_error)?;
            spans.push((source, start, count));
        }
        let ordinal_bytes = if ordinality.is_some() { std::mem::size_of::<Option<i32>>() + std::mem::size_of::<i32>() + 1 } else { 0 };
        let width = self.columns.len() + 1 + usize::from(ordinality.is_some());
        let bytes = rows.checked_mul(2 * std::mem::size_of::<(usize, usize)>() + ordinal_bytes)
            .and_then(|bytes| width.checked_mul(std::mem::size_of::<Column>()).and_then(|width| bytes.checked_add(width)))
            .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
        let mut reservation = memory.reserve(bytes)?;
        let mut parents = Vec::with_capacity(rows);
        let mut children = Vec::with_capacity(rows);
        let mut ordinals = if ordinality.is_some() { Vec::with_capacity(rows) } else { Vec::new() };
        for (parent, (source, start, count)) in spans.into_iter().enumerate() {
            for index in 0..count {
                context.check_cancelled_at(index)?;
                parents.push((0, parent));
                children.push((source, start + index));
                if ordinality.is_some() { ordinals.push(Some((index + 1) as i32)); }
            }
            if outer && count == 0 {
                parents.push((0, parent));
                children.push((0, 0));
                if ordinality.is_some() { ordinals.push(None); }
            }
        }
        let expanded = Column::Selected(SelectedColumn::new(sources, Arc::new(children)));
        let mut columns = Vec::with_capacity(width);
        for index in 0..self.columns.len() {
            let column = if rows == 0 { Column::Values(Vec::new()) } else {
                let (column, _scratch) = gather_column_rows(|_| self, &parents, index, memory)?;
                let lease = Arc::get_mut(&mut reservation).expect("Unnest owns the output reservation");
                lease.grow_to(lease.bytes().checked_add(column.retained_bytes()?).ok_or_else(size_error)?)?;
                column
            };
            columns.push(column);
        }
        columns.push(expanded);
        if let Some(name) = ordinality {
            let array = Arc::new(arrow_array::Int32Array::from(ordinals));
            let field = Arc::new(ArrowField::new(name, DataType::Int32, outer));
            let logical = LogicalType::Scalar(rtdb_types::ScalarType::I32);
            columns.push(Column::Arrow { array, field, logical: Some(if outer { logical.maybe() } else { logical }) });
        }
        Self::settled(columns, rows, reservation).map(Some)
    }

    pub(crate) fn append_column(&mut self, column: Column, mut reservation: Arc<MemoryReservation>, memory: &Arc<MemoryBudget>) -> Result<()> {
        use std::mem::size_of;
        let metadata = match &column {
            Column::Arrow { field, logical, .. } => field.size().checked_add(
                logical.as_ref().map_or(Ok(0), LogicalType::retained_bytes)?,
            ).ok_or_else(size_error)?,
            _ => 0,
        };
        let bytes = column.retained_bytes()?
            .checked_add(metadata)
            .and_then(|bytes| bytes.checked_add(size_of::<Column>() + size_of::<Arc<MemoryReservation>>() + 256))
            .ok_or_else(size_error)?;
        let lease = Arc::get_mut(&mut reservation).expect("An appended column owns its reservation");
        if bytes <= lease.bytes() { lease.shrink_to(bytes)?; } else { lease.grow_to(bytes)?; }
        let old_columns = self.columns.len().checked_mul(size_of::<Column>()).ok_or_else(size_error)?;
        let old_leases = self.appended.len().checked_mul(size_of::<Arc<MemoryReservation>>()).ok_or_else(size_error)?;
        let _scratch = memory.reserve(old_columns.checked_add(old_leases).ok_or_else(size_error)?)?;
        self.columns.reserve_exact(1);
        self.appended.reserve_exact(1);
        self.columns.push(column);
        self.appended.push(reservation);
        Ok(())
    }

    fn column_reservation(&self, position: usize) -> &Arc<MemoryReservation> {
        let offset = self.columns.len() - self.appended.len();
        position.checked_sub(offset).map_or(&self.reservation, |position| &self.appended[position])
    }

    /// A page of columns allocated under `reservation`, whose charge becomes exactly what they retain.
    pub fn settled(columns: Vec<Column>, len: usize, mut reservation: Arc<MemoryReservation>) -> Result<Self> {
        let base = columns.capacity().checked_mul(std::mem::size_of::<Column>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        let bytes = columns.iter().try_fold(base, |bytes, column| bytes.checked_add(column.retained_bytes()?).ok_or_else(size_error))?;
        let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("column-size", "A column page's charge is shared"))?;
        if bytes <= lease.bytes() { lease.shrink_to(bytes)?; } else { lease.grow_to(bytes)?; }
        Ok(Self { columns, len, reservation, appended: Vec::new() })
    }
    pub(crate) fn duplicate_for_replay(&self, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let base = self.columns.len().checked_mul(size_of::<Column>())
            .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        let bytes = self.columns.iter().try_fold(base, |bytes, column| {
            bytes.checked_add(column.retained_bytes()?).ok_or_else(size_error)
        })?;
        let reservation = memory.reserve(bytes)?;
        Self::settled(self.columns.iter().map(Column::duplicate).collect(), self.len, reservation)
    }

    fn charged(&self) -> usize { self.appended.iter().fold(self.reservation.bytes(), |bytes, lease| bytes.saturating_add(lease.bytes())) }

    pub(crate) fn validate_readable(&self, context: &crate::QueryContext) -> Result<()> {
        for row in 0..self.len {
            context.check_cancelled_at(row)?;
            for column in &self.columns { column.validate_readable_cell(row, &context.memory)?; }
        }
        Ok(())
    }

    pub(crate) fn working_set_bytes(&self, memory: &Arc<MemoryBudget>) -> Result<usize> {
        let selections = self.columns.iter().filter_map(|column| match column {
            Column::Selected(selected) => Some(selected),
            _ => None,
        });
        let sources = selections.clone().try_fold(0usize, |size, column| {
            size.checked_add(column.sources.len()).ok_or_else(size_error)
        })?;
        let _scratch = memory.reserve(sources.checked_mul(64).ok_or_else(size_error)?)?;
        let mut allocations = HashSet::new();
        let mut bytes = self.charged();
        for selected in selections {
            for (_, _, _, owner) in selected.sources.iter().flatten() {
                let held = match owner {
                    Some(owner) if Arc::ptr_eq(owner, &self.reservation) || self.appended.iter().any(|lease| Arc::ptr_eq(owner, lease)) => 0,
                    Some(owner) if allocations.insert(Arc::as_ptr(owner)) => owner.bytes(),
                    _ => 0,
                };
                bytes = bytes.checked_add(held).ok_or_else(size_error)?;
            }
        }
        Ok(bytes)
    }
    pub fn from_owned(batch: rtdb_columnar::OwnedBatch) -> Result<Self> {
        let schema = batch.batch().schema();
        let columns = schema.fields().iter().zip(batch.batch().columns()).map(|(field, array)| {
            Ok(Column::Arrow {
                array: Arc::clone(array),
                field: Arc::clone(field),
                logical: Some(rtdb_columnar::from_field(field)?),
            })
        }).collect::<Result<Vec<_>>>()?;
        Ok(Self { columns, len: batch.batch().num_rows(), reservation: Arc::clone(batch.reservation()), appended: Vec::new() })
    }

    pub(crate) fn collect_lists(
        &self, offsets: Vec<i32>, valid: Vec<bool>, field: &rtdb_plan::Field, memory: &Arc<MemoryBudget>,
    ) -> Result<(Column, Arc<MemoryReservation>)> {
        use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
        let physical = rtdb_columnar::to_field(&field.name, &field.data_type)?;
        let DataType::List(item) = physical.data_type() else { return Err(invalid("column-list", "Expected a list result field")); };
        let mut child_field = field.clone();
        child_field.name = item.name().clone();
        child_field.data_type = rtdb_columnar::from_field(item)?;
        let schema = rtdb_plan::Schema::new(vec![child_field])?;
        let record = if self.len == 0 { None } else { match self.physical_record(&schema, memory) {
            Ok(record) => record,
            Err(error) if error.code == "rtdb/memory-budget-exceeded" => None,
            Err(error) => return Err(error),
        } };
        let (child, item, mut reservation) = match record {
            Some((record, reservation)) => (Arc::clone(record.column(0)), Arc::clone(&record.schema_ref().fields()[0]), reservation),
            None if self.len == 0 => (arrow_array::new_empty_array(item.data_type()), Arc::clone(item), memory.reserve(item.size())?),
            None => match &self.columns[0] {
                Column::Selected(selected) => selected.list_values(item.name(), memory)?,
                Column::Arrow { array, field, .. } => {
                    let bytes = array.get_array_memory_size().checked_add(field.size()).ok_or_else(size_error)?;
                    (Arc::clone(array), Arc::clone(field), memory.reserve(bytes)?)
                }
                Column::Values(values) => {
                    let (column, charge) = pack_evaluated_column(values, memory)?;
                    match column {
                        Column::Arrow { array, field, .. } => (array, field, charge),
                        Column::Selected(selected) => selected.list_values(item.name(), memory)?,
                        Column::Values(_) => unreachable!("Evaluated cells are in Arrow"),
                    }
                }
            },
        };
        let lease = Arc::get_mut(&mut reservation).expect("The list owns its child reservation");
        let bytes = valid.len().checked_mul(32).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
        lease.grow_to(lease.bytes().checked_add(bytes).ok_or_else(size_error)?)?;
        let array = ListArray::try_new(item, OffsetBuffer::new(ScalarBuffer::from(offsets)), child, Some(NullBuffer::from(valid)))
            .map_err(|error| invalid("column-list", error))?;
        let field = Arc::new(ArrowField::new(physical.name(), array.data_type().clone(), physical.is_nullable() || array.null_count() != 0));
        let logical = rtdb_columnar::from_field(&field)?;
        Ok((Column::Arrow { array: Arc::new(array), field, logical: Some(logical) }, reservation))
    }

    pub(crate) fn pack_evaluated(&self, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bytes = self.columns.len().checked_mul(std::mem::size_of::<Column>() + std::mem::size_of::<Arc<MemoryReservation>>())
            .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        let reservation = memory.reserve(bytes)?;
        let mut columns = Vec::with_capacity(self.columns.len());
        let mut appended = Vec::with_capacity(self.columns.len());
        for (index, column) in self.columns.iter().enumerate() {
            let (column, charge) = match column {
                Column::Arrow { array, field, logical } => {
                    (Column::Arrow { array: Arc::clone(array), field: Arc::clone(field), logical: logical.clone() },
                     Arc::clone(self.column_reservation(index)))
                }
                Column::Selected(selected) => (Column::Selected(selected.clone()), Arc::clone(self.column_reservation(index))),
                Column::Values(values) => pack_evaluated_column(values, memory)?,
            };
            columns.push(column);
            appended.push(charge);
        }
        Ok(Self { columns, len: self.len, reservation, appended })
    }

    pub(crate) fn spill_record(&self, schema: &rtdb_plan::Schema, memory: &Arc<MemoryBudget>) -> Result<Option<rtdb_columnar::OwnedBatch>> {
        self.physical_record(schema, memory)?.map(|(batch, allocation)| rtdb_columnar::OwnedBatch::with_reservation(batch, allocation)).transpose()
    }

    pub(crate) fn physical_record(&self, schema: &rtdb_plan::Schema, memory: &Arc<MemoryBudget>)
        -> Result<Option<(arrow_array::RecordBatch, Arc<MemoryReservation>)>> {
        if self.columns.len() != schema.len() { return Ok(None); }
        let mut reservation = memory.reserve(crate::runtime_schema::schema_bound(schema)?.checked_add(8192).ok_or_else(size_error)?)?;
        let mut fields = Vec::with_capacity(self.columns.len());
        let mut arrays = Vec::with_capacity(self.columns.len());
        for (column, output) in self.columns.iter().zip(schema.fields()) {
            let (vector, _scratch) = match column {
                Column::Arrow { array, field, logical } => ((Arc::clone(array), Arc::clone(field), logical.clone()), None),
                Column::Selected(selected) => match selected.fragment_vector(0..self.len, memory) {
                    Ok((vector, scratch)) => (vector, Some(scratch)),
                    Err(error) if error.code == "rtdb.query/fragment-layout" => return Ok(None),
                    Err(error) => return Err(error),
                },
                Column::Values(values) => {
                    let (column, scratch) = pack_evaluated_column(values, memory)?;
                    let Column::Arrow { array, field, logical } = column else { return Ok(None); };
                    ((array, field, logical), Some(scratch))
                }
            };
            let (array, field, logical) = vector;
            if !logical.as_ref().is_some_and(|logical| *logical != LogicalType::Nothing && stable_read_layout(logical, &field)) {
                return Ok(None);
            }
            let bytes = array.get_array_memory_size().checked_add(field.size()).ok_or_else(size_error)?;
            let lease = Arc::get_mut(&mut reservation).expect("The spill page owns its reservation");
            lease.grow_to(lease.bytes().checked_add(bytes).ok_or_else(size_error)?)?;
            fields.push(field.as_ref().clone().with_name(output.name.clone()));
            arrays.push(array);
        }
        let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(self.len));
        let schema = Arc::new(arrow_schema::Schema::new(fields));
        let batch = arrow_array::RecordBatch::try_new_with_options(schema, arrays, &options).map_err(|error| invalid("spill-layout", error))?;
        Ok(Some((batch, reservation)))
    }

    pub(crate) fn from_list_parameter(
        source: &rtdb_columnar::OwnedBatch, position: usize, row: usize,
        parameter_type: &LogicalType, memory: &Arc<MemoryBudget>,
    ) -> Result<Option<Self>> {
        use arrow_array::cast::AsArray;
        let field = &source.batch().schema_ref().fields()[position];
        let mut reservation = memory.reserve(field.size().checked_mul(8).and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)?)?;
        if !stable_read_layout(parameter_type, field) || !readable_list_type(parameter_type, field) {
            return Ok(None);
        }
        let array = source.batch().column(position);
        if array.is_null(row) || *parameter_type == LogicalType::Null {
            return Self::settled(vec![Column::Values(Vec::new())], 0, memory.reserve(256)?).map(Some);
        }
        let (child, array) = match field.data_type() {
            DataType::List(child) => (child, array.as_list::<i32>().value(row)),
            DataType::FixedSizeList(child, _) => (child, array.as_fixed_size_list().value(row)),
            _ => return Ok(None),
        };
        let len = array.len();
        if len == 0 {
            return Self::settled(vec![Column::Values(Vec::new())], 0, reservation).map(Some);
        }
        let logical = rtdb_columnar::from_field(child)?;
        let metadata = logical.retained_bytes()?.checked_add(512).ok_or_else(size_error)?;
        Arc::get_mut(&mut reservation).expect("The list slice owns its metadata").shrink_to(metadata)?;
        let bound = array.get_array_memory_size().checked_add(4096).ok_or_else(size_error)?;
        let holds_unused_buffers = array.to_data().get_slice_memory_size().is_ok_and(|visible| {
            array.get_buffer_memory_size() > visible.saturating_mul(2).saturating_add(4096)
        });
        let (array, custody) = if holds_unused_buffers || bound > memory.available_bytes() {
            let Ok(compacted) = compact_arrow_rows(array.as_ref(), child, 0..len, memory) else { return Ok(None); };
            compacted
        } else {
            (array, Arc::clone(source.reservation()))
        };
        let columns = vec![Column::Arrow { field: Arc::clone(child), array, logical: Some(logical) }];
        Ok(Some(Self { columns, len, reservation, appended: vec![custody] }))
    }

    /// Gathers nonempty positions in already resolved batches without building row objects.
    pub fn take_rows(
        sources: &[impl std::borrow::Borrow<Self>], rows: &[(usize, usize)], positions: &[usize], memory: &Arc<MemoryBudget>,
    ) -> Result<Self> {
        Self::take_rows_with_custody(sources, rows, positions, false, memory)
    }

    pub(crate) fn select_keys(&self, rows: &[(usize, usize)], memory: &Arc<MemoryBudget>) -> Result<Self> {
        let positions: Vec<_> = (0..self.columns.len()).collect();
        if rows.len() < self.len {
            match Self::take_rows_with_custody(&[self], rows, &positions, false, memory) {
                Ok(output) => return Ok(output),
                Err(error) if error.code == "rtdb/memory-budget-exceeded" => {}
                Err(error) => return Err(error),
            }
        }
        Self::take_rows_with_custody(&[self], rows, &positions, true, memory)
    }

    pub(crate) fn key_projection(&self, positions: &[usize], memory: &Arc<MemoryBudget>) -> Result<Self> {
        self.project_keys(positions, false, memory)
    }

    pub(crate) fn compact_group_keys(&self, rows: &[(usize, usize)], memory: &Arc<MemoryBudget>) -> Result<Self> {
        let _positions = memory.reserve(self.columns.len().checked_mul(std::mem::size_of::<usize>()).ok_or_else(size_error)?)?;
        let positions: Vec<_> = (0..self.columns.len()).collect();
        let picked = Self::take_rows_with_custody(&[self], rows, &positions, true, memory)?;
        picked.project_keys(&positions, true, memory)
    }

    fn project_keys(&self, positions: &[usize], detach: bool, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let base = positions.len().checked_mul(std::mem::size_of::<Column>())
            .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        let mut reservation = memory.reserve(base)?;
        let mut columns = Vec::with_capacity(positions.len());
        for &position in positions {
            let column = &self.columns[position];
            let lease = Arc::get_mut(&mut reservation).expect("The key projection owns its reservation");
            if detach && let Column::Selected(selection) = column {
                let layout = |source: usize| selection.sources[source].as_ref().map(|(_, field, logical, _)| (field, logical));
                let first = selection.cells.first().and_then(|&(source, _)| layout(source));
                if selection.cells.iter().all(|&(source, _)| layout(source) == first) {
                    let ((array, field, logical), _scratch) = selection.fragment_vector(0..self.len, memory)?;
                    let detached = Column::Arrow { array, field, logical };
                    lease.grow_to(lease.bytes().checked_add(detached.retained_bytes()?).ok_or_else(size_error)?)?;
                    columns.push(detached);
                    continue;
                }
                let _boundaries = memory.reserve(self.len)?;
                let mut boundaries = vec![false; self.len];
                selection.fragment_boundaries(&mut boundaries);
                let ends = boundaries.into_iter().enumerate().filter_map(|(index, boundary)| boundary.then_some(index))
                    .chain(std::iter::once(self.len));
                let mut sources = Vec::new();
                let mut cells = Vec::with_capacity(self.len);
                let mut start = 0;
                for end in ends {
                    let ((array, field, logical), charge) = selection.fragment_vector(start..end, memory)?;
                    cells.extend((0..end - start).map(|row| (sources.len(), row)));
                    sources.push(Some((array, field, logical, Some(charge))));
                    start = end;
                }
                let detached = Column::Selected(SelectedColumn::new(sources, Arc::new(cells)));
                lease.grow_to(lease.bytes().checked_add(detached.retained_bytes()?).ok_or_else(size_error)?)?;
                columns.push(detached);
            } else {
                lease.grow_to(lease.bytes().checked_add(column.retained_bytes()?).ok_or_else(size_error)?)?;
                columns.push(column.duplicate());
            }
        }
        Self::settled(columns, self.len, reservation)
    }

    pub(crate) fn take_ranges(mut sources: Vec<Arc<Self>>, spans: &[Range<usize>], rows: usize, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        if sources.is_empty() { return Ok(None); }
        if sources.len() == 1 && spans[0] == (0..sources[0].len) {
            let batch = sources.pop().expect("The output holds one batch");
            return Arc::try_unwrap(batch).map(Some).map_err(|_| invalid("batch-state", "A complete output batch remains shared"));
        }
        let width = sources[0].columns.len();
        let bytes = rows.checked_mul(std::mem::size_of::<(usize, usize)>())
            .and_then(|bytes| bytes.checked_add(width.checked_mul(std::mem::size_of::<usize>())?)).ok_or_else(size_error)?;
        let _selection = memory.reserve(bytes)?;
        let picks = spans.iter().cloned().enumerate().flat_map(|(index, span)| span.map(move |row| (index, row))).collect::<Vec<_>>();
        let positions = (0..width).collect::<Vec<_>>();
        Self::take_rows(&sources, &picks, &positions, memory).map(Some)
    }

    pub(crate) fn take_rows_with_custody(
        sources: &[impl std::borrow::Borrow<Self>], rows: &[(usize, usize)], positions: &[usize],
        retain_sources: bool, memory: &Arc<MemoryBudget>,
    ) -> Result<Self> {
        let source = |batch: usize| sources[batch].borrow();
        if rows.is_empty() || sources.iter().map(|source| source.borrow()).any(|source| positions.iter().any(|&position| position >= source.columns.len()))
            || rows.iter().any(|&(batch, row)| sources.get(batch).is_none_or(|source| row >= source.borrow().len))
        {
            return Err(invalid("column-gather", "Row position is outside its source batch"));
        }
        let mut reservation = memory.reserve(positions.len().saturating_mul(256).saturating_add(256))?;
        let mut columns = Vec::with_capacity(positions.len());
        for &position in positions {
            let (column, _scratch) = gather_column_cells(source, rows.iter().copied().map(Some), rows.len(), position, retain_sources, memory)?;
            let retained = column.retained_bytes()?;
            let lease = Arc::get_mut(&mut reservation).expect("ordered gather owns its reservation");
            lease.grow_to(lease.bytes().checked_add(retained).ok_or_else(size_error)?)?;
            columns.push(column);
        }
        Self::settled(columns, rows.len(), reservation)
    }

    /// A page of rows as columns of values, charged as each column is built.
    pub fn from_page(page: &OwnedPage, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let width = page.rows().first().map_or(0, |row| row.values.len());
        let bound = width.checked_mul(std::mem::size_of::<Column>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        let mut reservation = memory.reserve(bound)?;
        let mut columns = Vec::with_capacity(width);
        for position in 0..width {
            let cells = page.len().checked_mul(std::mem::size_of::<Value>()).ok_or_else(size_error)?;
            let bytes = page.rows().iter().try_fold(cells, |bytes, row| {
                let value = row.values.get(position).ok_or_else(|| invalid("column-row", "A row is narrower than its page"))?;
                bytes.checked_add(value.retained_bytes()?).ok_or_else(size_error)
            })?;
            let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("column-size", "A column page's charge is shared"))?;
            lease.grow_to(lease.bytes().checked_add(bytes).ok_or_else(size_error)?)?;
            let mut values = Vec::with_capacity(page.len());
            values.extend(page.rows().iter().map(|row| row.values[position].clone()));
            columns.push(Column::Values(values));
        }
        let output = Self { columns, len: page.len(), reservation, appended: Vec::new() };
        output.pack_evaluated(memory)
    }

    pub(crate) fn empty_with_width(width: usize, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bytes = width.checked_mul(std::mem::size_of::<Column>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        let reservation = memory.reserve(bytes)?;
        Self::settled((0..width).map(|_| Column::Values(Vec::new())).collect(), 0, reservation)
    }

    /// Materialises every row, admitting the page's rows together before any is built.
    /// Rows share the page's charge, which ends when the last of them drops.
    pub fn rows(&self, memory: &Arc<MemoryBudget>) -> Result<OwnedPage> { self.rows_admitting(memory, false) }
    pub fn rows_admitting(&self, memory: &Arc<MemoryBudget>, admit_dates: bool) -> Result<OwnedPage> {
        let mut page = PageBuilder::new(self.len, memory)?;
        let mut bound = 0usize;
        for row in 0..self.len {
            let row_bound = self.columns.iter().try_fold(256usize, |bound, column| bound.checked_add(column.bound(row)?).ok_or_else(size_error))?;
            bound = bound.checked_add(row_bound).ok_or_else(size_error)?;
        }
        let reservation = memory.reserve(bound)?;
        for row in 0..self.len {
            let mut values = Vec::with_capacity(self.columns.len());
            for column in &self.columns {
                let mut value = column.value(row)?;
                if admit_dates { value.admit_expression_dates()?; }
                values.push(value);
            }
            page.push(OwnedRow::with_reservation(values, Arc::clone(&reservation))?)?;
        }
        Ok(page.finish())
    }
}

pub(crate) fn ordinal_column(len: usize, start: i64, name: &str, context: &crate::QueryContext)
    -> Result<(Column, Arc<MemoryReservation>)> {
    ordinal_column_by(len, name, context, |index| start + index as i64)
}

pub(crate) fn ordinal_column_by(len: usize, name: &str, context: &crate::QueryContext, mut ordinal: impl FnMut(usize) -> i64)
    -> Result<(Column, Arc<MemoryReservation>)> {
    let bound = len.checked_mul(std::mem::size_of::<i64>())
        .and_then(|bytes| bytes.checked_add(name.len())).and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)?;
    let reservation = context.memory.reserve(bound)?;
    let mut values = Vec::with_capacity(len);
    for index in 0..len {
        context.check_cancelled_at(index)?;
        values.push(ordinal(index));
    }
    let array = Arc::new(arrow_array::Int64Array::from(values));
    let field = Arc::new(ArrowField::new(name, DataType::Int64, false));
    let logical = Some(LogicalType::Scalar(rtdb_types::ScalarType::I64));
    Ok((Column::Arrow { array, field, logical }, reservation))
}

pub(crate) fn gather_column_rows<'a>(
    source: impl Fn(usize) -> &'a ColumnBatch, rows: &[(usize, usize)], position: usize, memory: &Arc<MemoryBudget>,
) -> Result<(Column, Arc<MemoryReservation>)> {
    gather_column_cells(source, rows.iter().copied().map(Some), rows.len(), position, false, memory)
}

pub(crate) fn gather_nullable_column<'a>(
    source: impl Fn(usize) -> &'a ColumnBatch, rows: &[Option<(usize, usize)>], position: usize, memory: &Arc<MemoryBudget>,
) -> Result<(Column, Arc<MemoryReservation>)> {
    let (column, mut reservation) = gather_column_cells(source, rows.iter().copied(), rows.len(), position, true, memory)?;
    let bytes = column.retained_bytes()?;
    let lease = Arc::get_mut(&mut reservation).expect("A nullable gather owns its reservation");
    if bytes <= lease.bytes() { lease.shrink_to(bytes)?; } else { lease.grow_to(bytes)?; }
    Ok((column, reservation))
}

#[test]
fn shifted_columns_share_the_input_reservations_across_appends() {
    use arrow_array::{StringArray, RecordBatch};
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::Schema;

    let bytes = 1 << 20;
    let memory = MemoryBudget::new(bytes * 3);
    let child = Arc::new(ArrowField::new("item", DataType::Utf8, true));
    let array = ListArray::new(Arc::clone(&child), OffsetBuffer::new(vec![0, 1].into()),
        Arc::new(StringArray::from(vec!["x".repeat(bytes)])), None);
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("list", DataType::List(child), false),
        ArrowField::new("text", DataType::Utf8, false),
    ]));
    let input = RecordBatch::try_new(schema, vec![Arc::new(array), Arc::new(StringArray::from(vec!["y".repeat(bytes)]))]).unwrap();
    let mut batch = ColumnBatch::from_owned(rtdb_columnar::OwnedBatch::new(input, &memory).unwrap()).unwrap();
    for _ in 0..8 {
        for position in [0, 1] {
            let (column, reservation) = gather_nullable_column(|_| &batch, &[Some((0, 0))], position, &memory).unwrap();
            batch.append_column(column, reservation, &memory).unwrap();
        }
    }
    assert_eq!(batch.working_set_bytes(&memory).unwrap(), batch.charged());
    assert!(memory.used_bytes() < bytes * 2 + 64 * 1024);
    drop(batch);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn projected_partition_keys_release_unrelated_source_buffers() {
    use arrow_array::Int64Array;
    use arrow_buffer::OffsetBuffer;

    let memory = MemoryBudget::new(3 << 20);
    let custody = memory.reserve(2 << 20).unwrap();
    let element = Arc::new(ArrowField::new("item", DataType::Int64, false));
    let array = Arc::new(ListArray::new(Arc::clone(&element), OffsetBuffer::new(vec![0, 1, 2].into()),
        Arc::new(Int64Array::from(vec![10, 20])), None));
    let field = Arc::new(ArrowField::new("key", DataType::List(element), false));
    let logical = rtdb_columnar::from_field(&field).unwrap();
    let selected = SelectedColumn::new(vec![Some((array, field, Some(logical), Some(custody)))], Arc::new(vec![(0, 1)]));
    let input = ColumnBatch::settled(vec![Column::Selected(selected)], 1, memory.reserve(1024).unwrap()).unwrap();
    let keys = input.compact_group_keys(&[(0, 0)], &memory).unwrap();
    drop(input);
    assert!(memory.used_bytes() < 64 * 1024);
    assert!(matches!(keys.columns[0], Column::Arrow { .. }));
    assert_eq!(keys.columns[0].value(0).unwrap(), Value::list(vec![Value::I64(20)]));
}

#[test]
fn dense_partition_keys_release_an_attached_page_reservation() {
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::Schema;

    let memory = MemoryBudget::new(3 << 20);
    let schema = Arc::new(Schema::new(vec![ArrowField::new("key", DataType::Int64, false)]));
    let record = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![10, 20]))]).unwrap();
    let custody = memory.reserve(2 << 20).unwrap();
    let input = ColumnBatch::from_owned(rtdb_columnar::OwnedBatch::with_reservation(record, custody).unwrap()).unwrap();
    let keys = input.compact_group_keys(&[(0, 1)], &memory).unwrap();
    drop(input);
    assert!(memory.used_bytes() < 64 * 1024);
    assert_eq!(keys.columns[0].value(0).unwrap(), Value::I64(20));
}

#[test]
fn mixed_gather_retains_arrow_payloads_alongside_evaluated_cells() {
    let memory = MemoryBudget::new(1 << 20);
    let embedding = Value::list(vec![Value::F32(-0.0), Value::F32(2.0)]);
    let logical = embedding.logical_type();
    let field = Arc::new(rtdb_columnar::to_field("value", &logical).unwrap());
    let array = rtdb_columnar::copy_array(&logical, std::slice::from_ref(&embedding)).unwrap();
    let arrow = Column::Arrow { array: Arc::clone(&array), field, logical: Some(logical) };
    let evaluated = Column::Values(vec![Value::I64(3)]);
    let batches = [arrow, evaluated].map(|column| {
        ColumnBatch::settled(vec![column], 1, memory.reserve(4096).unwrap()).unwrap()
    });
    let picks = [Some((1, 0)), Some((0, 0)), None, Some((0, 0))];
    let (column, _charge) = gather_column_cells(|index| &batches[index], picks.into_iter(), picks.len(), 0, true, &memory).unwrap();
    drop(batches);
    assert_eq!(column.value(0).unwrap(), Value::I64(3));
    assert_eq!(column.value(2).unwrap(), Value::Null);
    for row in [1, 3] {
        let (retained, _, _, offset) = column.arrow_cell(row).unwrap().unwrap();
        assert!(std::ptr::eq(retained, array.as_ref()));
        assert_eq!(offset, 0);
        assert_eq!(column.value(row).unwrap(), embedding);
    }
}

#[test]
fn fixed_size_vector_output_keeps_its_component_buffer() {
    let embedding = Value::FixedSizeList(rtdb_types::ListValue::from_f32(arrow_array::Float32Array::from(vec![Some(-0.0), None, Some(f32::MAX)])));
    let logical = embedding.logical_type().maybe();
    let field = rtdb_columnar::to_field("embedding", &logical).unwrap();
    let array = rtdb_columnar::copy_array(&logical, &[embedding.clone(), Value::Null]).unwrap();
    let column = Column::Arrow { array: Arc::clone(&array), field: Arc::new(field.clone()), logical: Some(logical.clone()) };
    let kept = conform(&column, &logical, &field).unwrap();
    let components = |array: &ArrayRef| array.as_any().downcast_ref::<FixedSizeListArray>().unwrap().values().to_data().buffers()[0].as_ptr();
    assert_eq!(components(&kept), components(&array));
    assert_eq!(kept.null_count(), 1);
}

#[test]
fn evaluated_vectors_with_absent_rows_and_components_pack_into_one_arrow_column() {
    let memory = MemoryBudget::new(1 << 20);
    let vector = |values: Vec<Option<f32>>| Value::List(rtdb_types::ListValue::from_f32(arrow_array::Float32Array::from(values)));
    let values = vec![vector(vec![Some(1.0), Some(-0.0)]), Value::Null, vector(vec![None, Some(f32::NAN)]), vector(vec![])];
    let (column, _charge) = pack_evaluated_column(&values, &memory).unwrap();
    let Column::Arrow { array, .. } = &column else { panic!("Expected one Arrow column") };
    let components = array.as_any().downcast_ref::<ListArray>().unwrap().values().as_any().downcast_ref::<arrow_array::Float32Array>().unwrap();
    assert_eq!(components.values().iter().map(|value| value.to_bits()).collect::<Vec<_>>()[..2], [1.0f32.to_bits(), (-0.0f32).to_bits()]);
    assert_eq!(components.values()[3].to_bits(), f32::NAN.to_bits());
    assert_eq!(column.value(1).unwrap(), Value::Null);
    assert_eq!(column.value(2).unwrap().list_view().unwrap().get(0).unwrap().into_owned(), Value::Null);
    assert_eq!(column.value(3).unwrap().list_view().unwrap().len(), 0);

    let mixed = vec![vector(vec![Some(1.0)]), Value::List(rtdb_types::ListValue::from_f64(arrow_array::Float64Array::from(vec![2.0])))];
    assert!(matches!(pack_evaluated_column(&mixed, &memory).unwrap().0, Column::Selected(_)));
}

#[test]
fn a_gather_retains_the_batch_reservation_for_a_repeated_vector() {
    let memory = MemoryBudget::new(1 << 20);
    let vector = Value::list((0..4096).map(|n| Value::F32(n as f32)).collect());
    let column = repeat_cell(&vector, 2).unwrap();
    let (array, ..) = column.arrow_cell(0).unwrap().unwrap();
    let bytes = array.get_array_memory_size();
    let batch = ColumnBatch::settled(vec![column], 2, memory.reserve(bytes * 2).unwrap()).unwrap();
    assert!(batch.column_reservation(0).bytes() >= bytes);
    let input = Arc::downgrade(batch.column_reservation(0));
    let (column, charge) = gather_column_rows(|_| &batch, &[(0, 1)], 0, &memory).unwrap();
    drop(batch);
    assert!(input.upgrade().is_some());
    assert_eq!(column.value(0).unwrap(), vector);
    drop((column, charge));
    assert_eq!(memory.used_bytes(), 0);
}

fn gather_column_cells<'a>(
    source: impl Fn(usize) -> &'a ColumnBatch, rows: impl Iterator<Item = Option<(usize, usize)>> + Clone, count: usize,
    position: usize, retain_sources: bool, memory: &Arc<MemoryBudget>,
) -> Result<(Column, Arc<MemoryReservation>)> {
    let source = |index| {
        let batch = source(index);
        (&batch.columns[position], batch.column_reservation(position))
    };
    let evaluated = rows.clone().flatten().filter(|&(index, _)| matches!(source(index).0, Column::Values(_)));
    let converted = if evaluated.clone().next().is_some() {
        let (len, bytes) = evaluated.clone().try_fold((0usize, 4096usize), |(len, bound), (index, row)| {
            let bytes = source(index).0.bound(row)?;
            Ok::<_, rtdb_types::Anomaly>((len.checked_add(1).ok_or_else(size_error)?, bound.checked_add(bytes).ok_or_else(size_error)?))
        })?;
        let scratch = memory.reserve(bytes)?;
        let mut values = Vec::with_capacity(len);
        for (index, row) in evaluated { values.push(source(index).0.value(row)?); }
        let packed = pack_evaluated_column(&values, memory)?;
        drop((values, scratch));
        Some(packed)
    } else { None };
    let mut next_evaluated = 0;
    let picks = rows.map(|cell| cell.map(|(index, row)| {
        let (column, charge) = source(index);
        if matches!(column, Column::Values(_)) {
            let (column, charge) = converted.as_ref().expect("Evaluated gather cells were packed");
            let row = next_evaluated;
            next_evaluated += 1;
            (column, charge, row)
        } else {
            (column, charge, row)
        }
    }));
    take_arrow_rows(picks, count, retain_sources, memory)
}

fn take_arrow_rows<'a>(
    rows: impl Iterator<Item = Option<(&'a Column, &'a Arc<MemoryReservation>, usize)>>, count: usize,
    retain_sources: bool, memory: &Arc<MemoryBudget>,
) -> Result<(Column, Arc<MemoryReservation>)> {
    let base = count.checked_mul(std::mem::size_of::<(usize, usize)>())
        .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
    let mut scratch = memory.reserve(base)?;
    let lease = Arc::get_mut(&mut scratch).expect("An Arrow gather owns its reservation");
    let mut sources = Vec::new();
    let mut positions = HashMap::new();
    let mut cells = Vec::with_capacity(count);
    let mut keep_selection = retain_sources;
    let mut name = "";
    let mut missing = None;
    for cell in rows {
        let Some((column, reservation, row)) = cell else {
            let index = *missing.get_or_insert_with(|| {
                let index = sources.len();
                sources.push(None);
                index
            });
            cells.push((index, 0));
            continue;
        };
        let (source, offset, vector, custody) = match column {
            Column::Arrow { array, field, logical } => {
                name = field.name();
                let null = array.data_type() == &DataType::Null;
                let absent = null && field.is_nullable() && logical.as_ref() != Some(&LogicalType::Nothing);
                keep_selection |= nested_layout(array.data_type()) || (null && !absent);
                if array.is_null(row) && matches!(array.data_type(), DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary) {
                    keep_selection |= flat_copy_bytes(field, array.as_ref(), row)? != 0;
                }
                let vector = (!absent).then_some((array, field, logical));
                (0, row, vector, Some(reservation))
            }
            Column::Selected(column) => {
                keep_selection = true;
                let (source, offset) = column.cells[row];
                let stored = column.sources[source].as_ref();
                let vector = stored.map(|(array, field, logical, _)| (array, field, logical));
                if let Some((_, field, _)) = vector { name = field.name(); }
                (source, offset, vector, stored.map(|(_, _, _, owner)| owner.as_ref().unwrap_or(reservation)))
            }
            Column::Values(_) => unreachable!("An Arrow gather reads only Arrow columns"),
        };
        let key = (std::ptr::from_ref(column), source);
        let input = if let Some(&input) = positions.get(&key) {
            input
        } else {
            let mut bytes = lease.bytes().checked_add(384).ok_or_else(size_error)?;
            if let Some((_, field, logical)) = vector {
                let schema = field.size().checked_mul(8).ok_or_else(size_error)?;
                let logical_size = logical.as_ref().map_or(Ok(0), LogicalType::retained_bytes)?;
                bytes = bytes.checked_add(schema).and_then(|bytes| bytes.checked_add(logical_size)).ok_or_else(size_error)?;
            }
            lease.grow_to(bytes)?;
            let input = sources.len();
            sources.push(vector.map(|(array, field, logical)| (Arc::clone(array), Arc::clone(field), logical.clone(), custody.cloned())));
            positions.insert(key, input);
            input
        };
        cells.push((input, offset));
    }
    let cells = Arc::new(cells);
    let has_missing_fixed_binary = sources.iter().any(Option::is_none) && sources.iter().flatten()
        .any(|(array, ..)| matches!(array.data_type(), DataType::FixedSizeBinary(_)));
    let column = if keep_selection || has_missing_fixed_binary {
        Column::Selected(SelectedColumn::new(sources, cells))
    } else {
        let base = cells.len().checked_mul(64).and_then(|bytes| lease.bytes().checked_add(bytes)).ok_or_else(size_error)?;
        let bound = cells.iter().try_fold(base, |bound, &(source, row)| {
            let Some((array, field, ..)) = &sources[source] else { return Ok(bound) };
            flat_copy_bytes(field, array.as_ref(), row)?.checked_mul(4)
                .and_then(|bytes| bound.checked_add(bytes)).ok_or_else(size_error)
        })?;
        lease.grow_to(bound)?;
        gather_cells(sources, &cells, name)?
    };
    Ok((column, scratch))
}

fn flat_copy_bytes(field: &ArrowField, array: &dyn Array, row: usize) -> Result<usize> {
    use arrow_array::cast::AsArray;
    Ok(match array.data_type() {
        DataType::Boolean => 1,
        DataType::Utf8 => array.as_string::<i32>().value(row).len(),
        DataType::Binary => array.as_binary::<i32>().value(row).len(),
        DataType::LargeUtf8 => array.as_string::<i64>().value(row).len(),
        DataType::LargeBinary => array.as_binary::<i64>().value(row).len(),
        DataType::FixedSizeBinary(width) => usize::try_from(*width).map_err(|_| size_error())?,
        ty => match ty.primitive_width() {
            Some(width) => width,
            None => rtdb_columnar::estimate_copy_cell_bytes(field, array, row)?,
        },
    })
}

/// The put leg each event batch stores a resolved row's cells in.
struct Source { batch: Arc<IndexBatch>, put: StructArray }

/// Gathers the scan's columns for rows that name their batch by its place in `sources`, in the order given.
pub(crate) fn gather(rows: &[ResolvedRow<(u32, u32)>], sources_in: &[Arc<IndexBatch>], columns: &[Field], memory: &Arc<MemoryBudget>,
    mut derived: impl FnMut(usize) -> Result<Option<Column>>) -> Result<ColumnBatch> {
    let mut sources: Vec<Source> = Vec::new();
    // Each batch's operation union, and the sources its put legs became, found once per batch rather than per row.
    let mut batches: Vec<(UnionArray, Vec<(i8, usize)>)> = Vec::new();
    let mut index = vec![usize::MAX; sources_in.len()];
    let mut cells = Vec::with_capacity(rows.len());
    for row in rows {
        let (handle, position) = (row.source.0 as usize, row.source.1 as usize);
        let row_batch = sources_in.get(handle).ok_or_else(|| invalid("column-gather", "A resolved row names no batch"))?;
        let batch = match index[handle] {
            usize::MAX => {
                batches.push((row_batch.view().operations()?.clone(), Vec::new()));
                index[handle] = batches.len() - 1;
                batches.len() - 1
            }
            batch => batch,
        };
        let (op, legs) = &mut batches[batch];
        let leg = op.type_id(position);
        let source = match legs.iter().find(|(known, _)| *known == leg) {
            Some((_, source)) => *source,
            None => {
                let put = op.child(leg).as_any().downcast_ref::<StructArray>().ok_or_else(|| invalid("event-schema", "Visible event is not a put struct"))?.clone();
                sources.push(Source { batch: Arc::clone(row_batch), put });
                legs.push((leg, sources.len() - 1));
                sources.len() - 1
            }
        };
        cells.push((source, op.value_offset(position)));
    }
    let cells = Arc::new(cells);
    // How much a gathered column holds depends on which cells it picks, so each is charged as soon as it is built.
    let mut reservation = memory.reserve(256)?;
    let mut output = Vec::with_capacity(columns.len());
    for (position, field) in columns.iter().enumerate() {
        let column = if let Some(column) = derived(position)? {
            column
        } else {
            match field.name.as_str() {
                "_valid_from" => times("_valid_from", rows.iter().map(|row| row.valid_time.lower()), false),
                "_valid_to" => times("_valid_to", rows.iter().map(|row| row.valid_time.upper()), true),
                "_system_from" => times("_system_from", rows.iter().map(|row| row.system_time.lower()), false),
                "_system_to" => times("_system_to", rows.iter().map(|row| row.system_time.upper()), true),
                "_iid" => {
                    let iids = rows.iter().map(|row| sources_in[row.source.0 as usize].header(row.source.1 as usize).map(|header| header.iid.0)).collect::<Result<Vec<_>>>()?;
                    let array = arrow_array::FixedSizeBinaryArray::try_from_iter(iids.iter()).map_err(|error| invalid("column-gather", error))?;
                    Column::Arrow { array: Arc::new(array), field: Arc::new(ArrowField::new("_iid", DataType::FixedSizeBinary(16), false)),
                        logical: Some(LogicalType::Scalar(rtdb_types::ScalarType::FixedSizeBinary(16))) }
                }
                name => stored(&sources, &cells, name)?,
            }
        };
        let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("column-size", "A column page's charge is shared"))?;
        lease.grow_to(lease.bytes().checked_add(column.retained_bytes()?).ok_or_else(size_error)?)?;
        output.push(column);
    }
    ColumnBatch::settled(output, rows.len(), reservation)
}

/// A temporal column as microsecond UTC timestamps; an upper bound at the end of time reads as null.
pub(crate) fn times(name: &str, times: impl Iterator<Item = Timestamp>, upper: bool) -> Column {
    let array = arrow_array::TimestampMicrosecondArray::from_iter(times.map(|time| (!(upper && time == Timestamp::MAX)).then_some(time.0))).with_timezone("UTC");
    let logical = LogicalType::Scalar(rtdb_types::ScalarType::Timestamp { unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) });
    Column::Arrow { array: Arc::new(array), field: Arc::new(ArrowField::new(name, DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())), upper)),
        logical: Some(if upper { logical.maybe() } else { logical }) }
}

fn stored(sources: &[Source], cells: &Arc<Vec<(usize, usize)>>, name: &str) -> Result<Column> {
    let children: Vec<Option<StoredVector>> = sources.iter().map(|source| {
        let DataType::Struct(fields) = source.put.data_type() else { return None };
        let index = fields.iter().position(|field| field.name() == name)?;
        Some((Arc::clone(source.put.column(index)), Arc::clone(&fields[index]), source.batch.put_field_type(index).cloned(),
            Some(Arc::clone(source.batch.arrow_reservation()))))
    }).collect();
    gather_cells(children, cells, name)
}

fn gather_cells(children: Vec<Option<StoredVector>>, cells: &Arc<Vec<(usize, usize)>>, name: &str) -> Result<Column> {
    let Some((_, field, logical, _)) = children.iter().flatten().next().cloned() else {
        return Ok(Column::Arrow {
            array: Arc::new(arrow_array::NullArray::new(cells.len())),
            field: Arc::new(ArrowField::new(name, DataType::Null, true)),
            logical: Some(LogicalType::Null),
        });
    };
    let uniform = children.iter().all(|child| child.as_ref().is_some_and(|(_, other, other_logical, _)| other == &field && other_logical == &logical));
    if uniform {
        let arrays: Vec<&dyn Array> = children.iter().flatten().map(|(array, ..)| array.as_ref()).collect();
        let flat_layout = flat(field.data_type());
        if flat_layout || arrays.iter().all(|array| can_interleave_nested(*array)) {
            match interleave(&arrays, cells) {
                Ok(array) => return Ok(Column::Arrow { array, field, logical }),
                Err(error) if flat_layout => return Err(error),
                Err(_) => {}
            }
        }
    }
    if !uniform {
        if let Some(column) = joined_nullable(&children, cells, &field, &logical)? { return Ok(column); }
        if let Some(column) = joined_union(&children, cells, name)? { return Ok(column); }
    }
    Ok(Column::Selected(SelectedColumn::new(children, Arc::clone(cells))))
}

fn joined_nullable(children: &[Option<StoredVector>], cells: &[(usize, usize)],
    field: &ArrowField, logical: &Option<LogicalType>) -> Result<Option<Column>> {
    let Some(logical) = logical else { return Ok(None) };
    if matches!(field.data_type(), DataType::Union(..)) { return Ok(None); }
    let nullable = logical.clone().maybe();
    for (array, other_field, other_logical, _) in children.iter().flatten() {
        if other_field.data_type() != field.data_type() || other_field.metadata() != field.metadata()
            || other_logical.as_ref().is_none_or(|other| other.clone().maybe() != nullable)
            || !can_interleave_nested(array.as_ref()) {
            return Ok(None);
        }
    }
    let selected: Vec<_> = cells.iter().map(|&(source, row)| {
        if children[source].is_some() { (source, row) } else { (children.len(), 0) }
    }).collect();
    let nulls = arrow_array::new_null_array(field.data_type(), usize::from(children.iter().any(Option::is_none)));
    let arrays: Vec<_> = children.iter().map(|child| child.as_ref().map_or(nulls.as_ref(), |(array, ..)| array.as_ref()))
        .chain(std::iter::once(nulls.as_ref())).collect();
    match interleave(&arrays, &selected) {
        Ok(array) => Ok(Some(Column::Arrow { array, field: Arc::new(field.clone().with_nullable(true)), logical: Some(nullable) })),
        Err(error) if flat(field.data_type()) => Err(error),
        Err(_) => Ok(None),
    }
}

fn joined_union(children: &[Option<StoredVector>], cells: &[(usize, usize)], name: &str) -> Result<Option<Column>> {
    enum Layout<'a> { Missing, Scalar(i8, &'a dyn Array), Union(&'a UnionArray, Vec<Option<i8>>) }
    let mut logicals = Vec::with_capacity(children.len());
    for child in children {
        match child {
            None => logicals.push(&LogicalType::Null),
            Some((_, field, Some(logical), _)) if flat(field.data_type()) => logicals.push(logical),
            _ => return Ok(None),
        }
    }
    let logical = LogicalType::join_all(logicals.iter().copied());
    let target = rtdb_columnar::to_field(name, &logical)?;
    let DataType::Union(legs, arrow_schema::UnionMode::Dense) = target.data_type() else { return Ok(None) };
    let leg_of = |field: &ArrowField| {
        let mut found = legs.iter().filter(|(_, leg)| leg.data_type() == field.data_type() && leg.metadata() == field.metadata());
        let (id, _) = found.next()?;
        found.next().is_none().then_some(id)
    };
    let null_leg = legs.iter().find(|(_, leg)| leg.data_type() == &DataType::Null).map(|(id, _)| id);
    let mut layouts = Vec::with_capacity(children.len());
    for child in children {
        let Some((array, field, ..)) = child else { layouts.push(Layout::Missing); continue; };
        layouts.push(match field.data_type() {
            DataType::Union(source_legs, _) => {
                let union = array.as_any().downcast_ref::<UnionArray>().ok_or_else(|| invalid("column-gather", "A union source is not a union"))?;
                let mut map = vec![None; source_legs.iter().map(|(id, _)| id as usize + 1).max().unwrap_or(0)];
                for (id, leg) in source_legs.iter() {
                    let Some(target) = leg_of(leg) else { return Ok(None) };
                    map[id as usize] = Some(target);
                }
                Layout::Union(union, map)
            }
            _ => match leg_of(field) { Some(leg) => Layout::Scalar(leg, array.as_ref()), None => return Ok(None) },
        });
    }
    let mut picks: Vec<Vec<(usize, usize)>> = vec![Vec::new(); legs.iter().map(|(id, _)| id as usize + 1).max().unwrap_or(0)];
    let mut ids = Vec::with_capacity(cells.len());
    let mut offsets = Vec::with_capacity(cells.len());
    for &(source, row) in cells {
        let (leg, offset) = match &layouts[source] {
            Layout::Scalar(leg, array) if !array.is_null(row) => (*leg, row),
            Layout::Missing | Layout::Scalar(..) => match null_leg { Some(leg) => (leg, row), None => return Ok(None) },
            Layout::Union(union, map) => match map.get(union.type_id(row) as usize).copied().flatten() {
                Some(leg) => (leg, union.value_offset(row)),
                None => return Err(invalid("column-gather", "A union cell names no leg")),
            },
        };
        let picked = &mut picks[leg as usize];
        offsets.push(i32::try_from(picked.len()).map_err(|_| invalid("column-gather", "A union leg exceeds i32 offsets"))?);
        picked.push((source, offset));
        ids.push(leg);
    }
    let children = legs.iter().map(|(id, leg)| -> Result<ArrayRef> {
        let picked = &picks[id as usize];
        if leg.data_type() == &DataType::Null { return Ok(Arc::new(arrow_array::NullArray::new(picked.len()))); }
        let empty = arrow_array::new_empty_array(leg.data_type());
        let arrays: Vec<&dyn Array> = layouts.iter().map(|layout| match layout {
            Layout::Scalar(source, array) if *source == id => *array,
            Layout::Missing | Layout::Scalar(..) => empty.as_ref(),
            Layout::Union(union, map) => map.iter().position(|target| *target == Some(id)).map_or(empty.as_ref(), |source| union.child(source as i8).as_ref()),
        }).collect();
        arrow_select::interleave::interleave(&arrays, picked).map_err(|error| invalid("column-gather", error))
    }).collect::<Result<Vec<_>>>()?;
    let array = UnionArray::try_new(legs.clone(), datafusion_common::arrow::buffer::ScalarBuffer::from(ids), Some(datafusion_common::arrow::buffer::ScalarBuffer::from(offsets)), children)
        .map_err(|error| invalid("column-gather", error))?;
    Ok(Some(Column::Arrow { array: Arc::new(array), field: Arc::new(target), logical: Some(logical) }))
}

pub(crate) fn interleave(arrays: &[&dyn Array], cells: &[(usize, usize)]) -> Result<ArrayRef> {
    rtdb_columnar::interleave(arrays, cells).map_err(|error| invalid("column-gather", error))
}

#[test]
fn interleaving_a_contiguous_range_keeps_its_input_buffer() {
    let values = arrow_array::Int64Array::from(vec![10, 20, 30, 40]);
    let selected = interleave(&[&values], &[(0, 1), (0, 2)]).unwrap();
    let selected = selected.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
    assert_eq!(selected.values().as_ref(), &[20, 30]);
    assert_eq!(selected.values().as_ptr(), values.values()[1..].as_ptr());
}

#[test]
fn fragment_frames_keep_source_layouts_and_selection_order() {
    use rtdb_types::StructFields;

    let memory = MemoryBudget::new(1 << 22);
    let first = Value::Struct(StructFields::from([("a".into(), Value::U64(u64::MAX))]));
    let second = Value::Struct(StructFields::from([("b".into(), Value::Utf8("two".into()))]));
    let source = |value: &Value| {
        let ty = value.logical_type();
        let field = Arc::new(rtdb_columnar::to_field("doc", &ty).unwrap());
        let data = rtdb_columnar::build_array(&ty, std::slice::from_ref(value)).unwrap();
        Some((data, field, Some(ty), None))
    };
    let sources = vec![source(&first), source(&second), source(&first), None];
    let cells = Arc::new(vec![(2, 0), (0, 0), (1, 0), (3, 0), (2, 0)]);
    let selected = gather_cells(sources, &cells, "doc").unwrap();
    assert!(matches!(selected, Column::Selected(_)));
    let ids = Column::Arrow {
        array: Arc::new(arrow_array::Int64Array::from(vec![0, 1, 2, 3, 4])),
        field: Arc::new(ArrowField::new("_id", DataType::Int64, false)),
        logical: None,
    };
    let width = 1 << 16;
    let wide = Value::FixedSizeBinary(vec![31; width]);
    let union_values = [Value::I64(11), Value::I64(22), Value::I64(33), wide.clone(), Value::I64(44)];
    let union_type = union_values[0].logical_type().join(&wide.logical_type());
    let union = Column::Arrow {
        array: rtdb_columnar::build_array(&union_type, &union_values).unwrap(),
        field: Arc::new(rtdb_columnar::to_field("union", &union_type).unwrap()),
        logical: Some(union_type),
    };
    let batch = ColumnBatch::settled(vec![ids, selected, union], cells.len(), memory.reserve(256).unwrap()).unwrap();
    let mut wire = crate::fragment::FragmentBytes::new(&memory).unwrap();

    crate::fragment::encode_batch(&batch, &memory, &mut wire).unwrap();
    let wire = wire.into_bytes();
    assert!(wire.len() < width * 2);
    let frames = crate::fragment::decode_batches(&wire, &memory).unwrap();

    assert_eq!(frames.iter().map(|frame| frame.len).collect::<Vec<_>>(), [2, 1, 1, 1]);
    for frame in &frames {
        let field = match &frame.columns[1] {
            Column::Selected(column) => &column.source(0).unwrap().unwrap().0.1,
            Column::Arrow { field, .. } => field,
            Column::Values(_) => panic!("A selected field must remain Arrow"),
        };
        assert_eq!(field.name(), "doc");
    }
    let Column::Selected(column) = &frames[0].columns[1] else { unreachable!() };
    let ((array, ..), _) = column.source(0).unwrap().unwrap();
    let structure = array.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(structure.column(0).data_type(), &DataType::UInt64);
    let actual = frames.iter().flat_map(|frame| {
        (0..frame.len).map(|row| {
            [frame.columns[0].value(row).unwrap(), frame.columns[1].value(row).unwrap(), frame.columns[2].value(row).unwrap()]
        })
    }).collect::<Vec<_>>();
    let expected = (0..batch.len).map(|row| {
        [batch.columns[0].value(row).unwrap(), batch.columns[1].value(row).unwrap(), batch.columns[2].value(row).unwrap()]
    }).collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[test]
fn a_fragment_splits_repeated_positions_at_the_run_end_width() {
    use arrow_array::{RunArray, types::Int16Type};
    use rtdb_types::{RunEndWidth, ScalarType};

    let memory = MemoryBudget::new(1 << 28);
    let ty = LogicalType::RunEndEncoded { width: RunEndWidth::I16, value: Box::new(LogicalType::Scalar(ScalarType::I64)) };
    let field = Arc::new(rtdb_columnar::to_field("run", &ty).unwrap());
    let array = rtdb_columnar::build_array(&ty, &[Value::I64(7)]).unwrap();
    let len = (i16::MAX as usize) + 1;
    let selected = Column::Selected(SelectedColumn {
        sources: Arc::new(vec![Some((array, field, Some(ty), None))]),
        cells: Arc::new(vec![(0, 0); len]),
    });
    let batch = ColumnBatch::settled(vec![selected], len, memory.reserve(256).unwrap()).unwrap();
    let mut wire = crate::fragment::FragmentBytes::new(&memory).unwrap();

    crate::fragment::encode_batch(&batch, &memory, &mut wire).unwrap();
    let frames = crate::fragment::decode_batches(&wire.into_bytes(), &memory).unwrap();

    assert_eq!(frames.len(), 2);
    assert_eq!(frames.iter().map(|frame| frame.len).sum::<usize>(), len);
    for frame in frames {
        let Column::Selected(column) = &frame.columns[0] else { panic!("Run-end encoding must survive a fragment") };
        let ((array, ..), _) = column.source(0).unwrap().unwrap();
        let run = array.as_any().downcast_ref::<RunArray<Int16Type>>().unwrap();
        assert_eq!(run.run_ends().values(), &[frame.len as i16]);
        assert_eq!(frame.columns[0].value(frame.len - 1).unwrap(), Value::I64(7));
    }
}

#[test]
fn fragment_refiltering_retains_the_decoded_arrow_array() {
    use rtdb_types::ScalarType;

    let memory = MemoryBudget::new(128 * 1024);
    let width = 1 << 20;
    let ty = LogicalType::List { kind: ListKind::FixedSizeList(1), element: Box::new(LogicalType::from_legs([
        LogicalType::Scalar(ScalarType::I64), LogicalType::Scalar(ScalarType::FixedSizeBinary(width)),
    ])) };
    let field = Arc::new(rtdb_columnar::to_field("v", &ty).unwrap());
    let values = [Value::FixedSizeList(vec![Value::I64(2)].into()), Value::FixedSizeList(vec![Value::I64(3)].into())];
    let array = rtdb_columnar::build_array(&ty, &values).unwrap();
    let column = Column::Arrow { array, field, logical: Some(ty) };
    let page = ColumnBatch::settled(vec![column], 2, memory.reserve(256).unwrap()).unwrap();
    let mut wire = crate::fragment::FragmentBytes::new(&memory).unwrap();
    crate::fragment::encode_batch(&page, &memory, &mut wire).unwrap();
    let mut decoded = crate::fragment::decode_batches(wire.as_slice(), &memory).unwrap();
    let page = decoded.pop().unwrap();
    let Column::Selected(column) = &page.columns[0] else { panic!("Expected a selection over the decoded array") };
    let source = Arc::as_ptr(&column.source(0).unwrap().unwrap().0.0);

    let filtered = page.select(&[false, true], &memory).unwrap();

    let Column::Selected(column) = &filtered.columns[0] else { panic!("Filtering should preserve the Arrow selection") };
    let ((array, ..), row) = column.source(0).unwrap().unwrap();
    assert!(std::ptr::addr_eq(Arc::as_ptr(array), source));
    assert_eq!(row, 1);
    let fixed = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
    let union = fixed.values().as_any().downcast_ref::<UnionArray>().unwrap();
    assert!(union.child(1).is_empty());
    assert_eq!(filtered.columns[0].value(0).unwrap(), values[1]);
}

#[test]
fn ordered_gather_composes_previous_selections_and_keeps_arrow_custody() {
    use rtdb_types::ScalarType;

    let memory = MemoryBudget::new(2 << 20);
    let logical = LogicalType::list(LogicalType::Scalar(ScalarType::Binary));
    let wide = Value::list(vec![Value::Binary(vec![7; 1 << 20])]);
    let narrow = Value::list(vec![Value::Binary(vec![3; 4])]);
    let field = Arc::new(rtdb_columnar::to_field("payload", &logical).unwrap());
    let array = rtdb_columnar::build_array(&logical, std::slice::from_ref(&wide)).unwrap();
    let buffers = array.as_any().downcast_ref::<ListArray>().unwrap().values().to_data();
    let data = buffers.buffers()[1].as_ptr();
    let direct = Column::Arrow { array, field: Arc::clone(&field), logical: Some(logical.clone()) };

    let array = rtdb_columnar::build_array(&logical, std::slice::from_ref(&narrow)).unwrap();
    let lease = memory.reserve(array.get_array_memory_size()).unwrap();
    let selected = Column::Selected(SelectedColumn {
        sources: Arc::new(vec![None, Some((array, field, Some(logical), Some(lease)))]),
        cells: Arc::new(vec![(0, 0), (1, 0)]),
    });
    let first = ColumnBatch::settled(vec![direct], 1, memory.reserve(256).unwrap()).unwrap();
    let second = ColumnBatch::settled(vec![selected], 2, memory.reserve(256).unwrap()).unwrap();
    drop(buffers);
    let inputs = [first, second];

    let gathered = ColumnBatch::take_rows(&inputs, &[(1, 1), (0, 0), (1, 0), (1, 1), (0, 0)], &[0], &memory).unwrap();
    drop(inputs);

    let Column::Selected(selected) = &gathered.columns[0] else { panic!("Expected Arrow positions") };
    let ((array, ..), row) = selected.source(1).unwrap().unwrap();
    let output = array.as_any().downcast_ref::<ListArray>().unwrap().values().to_data();
    assert_eq!(output.buffers()[1].as_ptr(), data);
    assert_eq!(row, 0);
    assert!(memory.used_bytes() >= 1 << 20);
    let payload = gathered.working_set_bytes(&memory).unwrap() - gathered.charged();
    assert!(payload >= 1 << 20);
    let duplicate = ColumnBatch::settled(
        vec![gathered.columns[0].duplicate(), gathered.columns[0].duplicate()], 5, memory.reserve(256).unwrap(),
    ).unwrap();
    assert_eq!(duplicate.working_set_bytes(&memory).unwrap() - duplicate.charged(), payload);
    assert_eq!((0..5).map(|row| gathered.columns[0].value(row).unwrap()).collect::<Vec<_>>(),
        [narrow.clone(), wide.clone(), Value::Null, narrow, wide]);
    drop(duplicate);
    drop((output, gathered));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn ordered_gather_keeps_missing_fixed_binary_as_positions() {
    use rtdb_types::ScalarType;

    let memory = MemoryBudget::new(2 << 20);
    let ty = LogicalType::Scalar(ScalarType::FixedSizeBinary(1 << 20));
    let value = Value::FixedSizeBinary(vec![1; 1 << 20]);
    let column = Column::Arrow {
        array: rtdb_columnar::build_array(&ty, std::slice::from_ref(&value)).unwrap(),
        field: Arc::new(rtdb_columnar::to_field("payload", &ty).unwrap()),
        logical: Some(ty),
    };
    let null = Column::Arrow {
        array: Arc::new(arrow_array::NullArray::new(1)),
        field: Arc::new(ArrowField::new("payload", DataType::Null, true)),
        logical: Some(LogicalType::Null),
    };
    let page = |column| ColumnBatch::settled(vec![column], 1, memory.reserve(256).unwrap()).unwrap();
    let inputs = [page(column), page(null)];

    let gathered = ColumnBatch::take_rows(&inputs, &[(1, 0), (0, 0)], &[0], &memory).unwrap();

    assert!(matches!(gathered.columns[0], Column::Selected(_)));
    assert_eq!(gathered.columns[0].value(0).unwrap(), Value::Null);
    assert_eq!(gathered.columns[0].value(1).unwrap(), value);
}

#[test]
fn a_flat_gather_admits_many_matches_at_arrow_width() {
    let memory = MemoryBudget::new(16 << 20);
    let column = Column::Arrow {
        array: Arc::new(arrow_array::Int64Array::from(vec![42])),
        field: Arc::new(ArrowField::new("value", DataType::Int64, false)),
        logical: None,
    };
    let page = ColumnBatch::settled(vec![column], 1, memory.reserve(256).unwrap()).unwrap();
    let rows = vec![(0, 0); 65_536];

    let output = ColumnBatch::take_rows(&[page], &rows, &[0], &memory).unwrap();

    let Column::Arrow { array, .. } = &output.columns[0] else { panic!("Expected an Arrow vector") };
    let array = array.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
    assert_eq!(array.len(), rows.len());
    assert!(array.values().iter().all(|&value| value == 42));
}

#[test]
fn sparse_fixed_lists_copy_only_the_active_union_children() {
    use rtdb_types::ScalarType;

    let width = 1 << 20;
    let logical = LogicalType::List { kind: ListKind::FixedSizeList(1), element: Box::new(LogicalType::from_legs([
        LogicalType::Scalar(ScalarType::I64), LogicalType::Scalar(ScalarType::FixedSizeBinary(width)),
    ])) };
    let values = [Value::I64(1), Value::FixedSizeBinary(vec![0; width as usize]), Value::I64(3)].into_iter()
        .map(|value| Value::FixedSizeList(vec![value].into())).collect::<Vec<_>>();
    let array = rtdb_columnar::build_array(&logical, &values).unwrap();

    let selected = copy_selected_cells(&[array.as_ref()], &[(0, 0), (0, 2)]).unwrap();

    let lists = selected.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
    let union = lists.values().as_any().downcast_ref::<UnionArray>().unwrap();
    assert_eq!(union.child(1).len(), 0);
    assert!(selected.get_array_memory_size() < width as usize);
    let field = rtdb_columnar::to_field("v", &logical).unwrap();
    assert_eq!(rtdb_columnar::read_value(&field, selected.as_ref(), 0).unwrap(), values[0]);
    assert_eq!(rtdb_columnar::read_value(&field, selected.as_ref(), 1).unwrap(), values[2]);
}

#[test]
fn selecting_one_map_compacts_the_source_buffers() {
    let map = |text: String| Value::Map { entries: vec![(Value::Utf8("k".into()), Value::Utf8(text))], sorted: false };
    let small = map("small".into());
    let large = map("x".repeat(1 << 20));
    let logical = small.logical_type();
    let field = Arc::new(rtdb_columnar::to_field("v", &logical).unwrap());
    let array = rtdb_columnar::build_array(&logical, &[large, small.clone()]).unwrap();
    let column = Column::Selected(SelectedColumn {
        sources: Arc::new(vec![Some((array, Arc::clone(&field), Some(logical.clone()), None))]),
        cells: Arc::new(vec![(0, 1)]),
    });
    let memory = MemoryBudget::new(64 * 1024);

    let (result, _scratch) = relayout_output(&column, &logical, &field, &memory).unwrap().unwrap();

    assert!(result.as_any().is::<MapArray>());
    assert!(result.get_array_memory_size() < memory.limit_bytes());
    assert_eq!(rtdb_columnar::read_value(&field, result.as_ref(), 0).unwrap(), small);
}

#[test]
fn stored_list_projection_uses_arrow_for_contiguous_and_sparse_rows() {
    use arrow_array::Int64Array;
    use rtdb_index::{StoredEvent, StoredOperation};
    use rtdb_types::{Iid, Interval, ScalarType, StructFields};

    let memory = MemoryBudget::new(1 << 22);
    let logical = LogicalType::list(LogicalType::Scalar(ScalarType::I64));
    let field = Field {
        id: rtdb_plan::ColumnId(0),
        name: "items".into(),
        qualifier: None,
        qualified_only: false,
        unnamed: false,
        data_type: logical.clone(),
    };
    let events: Vec<_> = (0..3).map(|number| StoredEvent { position: None,
        origin: None,
        commit_position: 0,
        entity_key: None,
        iid: Iid([number as u8; 16]),
        system_from: Timestamp(10),
        valid_time: Interval::ALL,
        operation: StoredOperation::Put(StructFields::from([(
            field.name.clone(), Value::list(vec![Value::I64(number)]),
        )])),
        recency: None,
    }).collect();
    let declared = StructFields::from([(field.name.clone(), logical.clone())]);
    let source = IndexBatch::copy_events_with_put_schema(&events, &declared, &memory).unwrap();
    let ops = source.view().operations().unwrap();
    let put = ops.child(ops.type_id(0)).as_any().downcast_ref::<StructArray>().unwrap();
    let values = put.column(0).as_any().downcast_ref::<ListArray>().unwrap().values();
    let input = values.as_any().downcast_ref::<Int64Array>().unwrap().values();
    let arrow_field = rtdb_columnar::to_field(&field.name, &logical).unwrap();

    for selection in [[0, 1], [0, 2]] {
        let rows = selection.map(|row| ResolvedRow {
            source: (0, row),
            valid_time: Interval::ALL,
            system_time: Interval::ALL,
        });
        let batch = gather(&rows, std::slice::from_ref(&source), std::slice::from_ref(&field), &memory, |_| Ok(None)).unwrap();
        let array = conform(&batch.columns[0], &logical, &arrow_field).unwrap();
        let output = array.as_any().downcast_ref::<ListArray>().unwrap();
        for (index, value) in selection.into_iter().enumerate() {
            let values = output.value(index);
            assert_eq!(values.as_any().downcast_ref::<Int64Array>().unwrap().values().as_ref(), &[i64::from(value)]);
        }
        if selection == [0, 1] {
            let output_values = output.values().as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(output_values.values().as_ptr(), input.as_ptr());
        }
    }
}

#[test]
fn result_projection_keeps_a_scalar_union_already_in_its_output_layout() {
    use rtdb_types::ScalarType;

    let logical = LogicalType::from_legs([
        LogicalType::Scalar(ScalarType::I64),
        LogicalType::Scalar(ScalarType::Utf8),
        LogicalType::Null,
    ]);
    let field = Arc::new(rtdb_columnar::to_field("v", &logical).unwrap());
    let input = rtdb_columnar::copy_array(&logical, &[Value::I64(1), Value::Null, Value::Utf8("x".into())]).unwrap();
    let column = Column::Arrow {
        array: Arc::clone(&input),
        field: Arc::clone(&field),
        logical: Some(logical.clone()),
    };

    let result = conform(&column, &logical, &field).unwrap();

    assert!(Arc::ptr_eq(&input, &result));
}

#[test]
fn stored_nested_union_projection_keeps_arrow_for_sparse_rows() {
    use rtdb_index::{StoredEvent, StoredOperation};
    use rtdb_types::{Iid, Interval, StructFields};

    let memory = MemoryBudget::new(1 << 22);
    let record = Value::Struct(StructFields::from([("a".into(), Value::I64(1))]));
    let list = Value::list(vec![Value::I64(2)]);
    let logical = LogicalType::from_legs([record.logical_type(), list.logical_type(), LogicalType::Null]);
    let field = Field {
        id: rtdb_plan::ColumnId(0), name: "v".into(), qualifier: None,
        qualified_only: false, unnamed: false, data_type: logical.clone(),
    };
    let events = [record.clone(), Value::Null, list.clone()].into_iter().enumerate().map(|(row, value)| StoredEvent { position: None,
        origin: None,
        commit_position: 0,
        entity_key: None,
        iid: Iid([row as u8; 16]), system_from: Timestamp(10), valid_time: Interval::ALL, recency: None,
        operation: StoredOperation::Put(StructFields::from([("v".into(), value)])),
    }).collect::<Vec<_>>();
    let put_types = StructFields::from([("v".into(), logical.clone())]);
    let page = IndexBatch::copy_events_with_put_schema(&events, &put_types, &memory).unwrap();
    let rows = [2, 0].map(|row| ResolvedRow { source: (0, row), valid_time: Interval::ALL, system_time: Interval::ALL });

    let batch = gather(&rows, &[page], &[field], &memory, |_| Ok(None)).unwrap();

    let Column::Arrow { array, .. } = &batch.columns[0] else { panic!("Expected a projected Arrow union"); };
    let arrow_field = rtdb_columnar::to_field("v", &logical).unwrap();
    let retained = conform(&batch.columns[0], &logical, &arrow_field).unwrap();
    assert!(Arc::ptr_eq(array, &retained));
    assert_eq!(rtdb_columnar::read_value(&arrow_field, retained.as_ref(), 0).unwrap(), list);
    assert_eq!(rtdb_columnar::read_value(&arrow_field, retained.as_ref(), 1).unwrap(), record);
}

#[test]
fn projections_keep_typed_arrays_when_a_page_lacks_the_column() {
    use rtdb_types::ScalarType;

    let int = LogicalType::Scalar(ScalarType::I64);
    let text = LogicalType::Scalar(ScalarType::Utf8);
    let list = LogicalType::list(int.clone());
    let column = |logical: &LogicalType, value| {
        Some((rtdb_columnar::copy_array(logical, &[value]).unwrap(),
            Arc::new(rtdb_columnar::to_field("v", logical).unwrap()), Some(logical.clone()), None))
    };

    let lists = [None, column(&list, Value::list(vec![Value::I64(2)]))];
    let (_, field, logical, _) = lists[1].as_ref().unwrap();
    let list = joined_nullable(&lists, &[(0, 0), (1, 0)], field, logical).unwrap().unwrap();
    assert!(matches!(list, Column::Arrow { .. }));
    assert_eq!(list.value(0).unwrap(), Value::Null);
    assert_eq!(list.value(1).unwrap(), Value::list(vec![Value::I64(2)]));

    let scalars = [column(&int, Value::I64(1)), None, column(&text, Value::Utf8("three".into()))];
    let union = joined_union(&scalars, &[(2, 0), (1, 0), (0, 0)], "v").unwrap().unwrap();
    assert!(matches!(union, Column::Arrow { .. }));
    assert_eq!(union.value(0).unwrap(), Value::Utf8("three".into()));
    assert_eq!(union.value(1).unwrap(), Value::Null);
    assert_eq!(union.value(2).unwrap(), Value::I64(1));
}

#[test]
fn a_projection_absent_from_every_page_is_an_arrow_null_array() {
    let column = stored(&[], &Arc::new(vec![(0, 0), (0, 1)]), "missing").unwrap();
    assert!(matches!(&column, Column::Arrow { array, .. } if array.data_type() == &DataType::Null && array.len() == 2));
    assert_eq!(column.value(0).unwrap(), Value::Null);
}

#[test]
fn a_mixed_struct_selection_preserves_source_values_and_aligns_arrow_output() {
    use rtdb_index::{StoredEvent, StoredOperation};
    use rtdb_types::{Iid, Interval, StructFields};

    let memory = MemoryBudget::new(1 << 22);
    let a = Value::Struct(StructFields::from([("a".into(), Value::I64(7))]));
    let b = Value::Struct(StructFields::from([("b".into(), Value::Bool(true))]));
    let source = |id, value: Option<Value>| {
        let fields: StructFields<Value> = value.into_iter().map(|value| ("v".into(), value)).collect();
        let schema = fields.iter().map(|(name, value)| (name.clone(), value.logical_type())).collect();
        let event = StoredEvent { position: None, origin: None, commit_position: 0, entity_key: None, iid: Iid([id; 16]), system_from: Timestamp(10), valid_time: Interval::ALL,
            operation: StoredOperation::Put(fields), recency: None };
        IndexBatch::copy_events_with_put_schema(&[event], &schema, &memory).unwrap()
    };
    let sources = [source(0, Some(a.clone())), source(1, Some(b.clone())), source(2, None)];
    let logical = a.logical_type().join(&b.logical_type()).maybe();
    let field = Field { id: rtdb_plan::ColumnId(0), name: "v".into(), qualifier: None, qualified_only: false, unnamed: false,
        data_type: logical.clone() };
    let rows = [1, 2, 0, 1].map(|source| ResolvedRow { source: (source, 0), valid_time: Interval::ALL, system_time: Interval::ALL });
    let page = gather(&rows, &sources, &[field], &memory, |_| Ok(None)).unwrap();
    let Column::Selected(selected) = &page.columns[0] else { panic!("Expected source vectors with selected positions"); };
    assert_eq!(selected.null_test(true).unwrap(), [Some(false), Some(true), Some(false), Some(false)]);
    assert_eq!(page.columns[0].value(0).unwrap(), b);
    assert_eq!(page.columns[0].value(1).unwrap(), Value::Null);
    assert_eq!(page.columns[0].bound(1).unwrap(), std::mem::size_of::<Value>());
    assert_eq!(page.columns[0].value(2).unwrap(), a);
    let page = page.select(&[true, true, true, false], &memory).unwrap();
    drop(sources);

    let arrow = rtdb_columnar::to_field("v", &logical).unwrap();
    let (result, _scratch) = relayout_output(&page.columns[0], &logical, &arrow, &memory).unwrap().unwrap();

    let structs = result.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(structs.column_names(), ["a", "b"]);
    assert_eq!(structs.len(), 3);
    assert!(structs.is_null(1));
    assert_eq!(structs.column(0).as_any().downcast_ref::<arrow_array::Int64Array>().unwrap().iter().collect::<Vec<_>>(), [None, None, Some(7)]);
    assert_eq!(structs.column(1).as_any().downcast_ref::<arrow_array::BooleanArray>().unwrap().iter().collect::<Vec<_>>(), [Some(true), None, None]);
}

#[test]
fn selected_arrow_output_preserves_the_bottom_cell_read_error() {
    let target = LogicalType::from_legs([
        LogicalType::Scalar(rtdb_types::ScalarType::I64),
        LogicalType::list(LogicalType::Scalar(rtdb_types::ScalarType::Bool)),
        LogicalType::Null,
    ]);
    let source = Arc::new(rtdb_columnar::to_field("v", &LogicalType::Nothing).unwrap());
    let column = Column::Selected(SelectedColumn {
        sources: Arc::new(vec![Some((Arc::new(arrow_array::NullArray::new(1)), source, Some(LogicalType::Nothing), None))]),
        cells: Arc::new(vec![(0, 0)]),
    });
    let field = rtdb_columnar::to_field("v", &target).unwrap();
    let memory = MemoryBudget::new(1 << 20);

    assert!(relayout_output(&column, &target, &field, &memory).unwrap().is_none());
    assert_eq!(column.value(0).unwrap_err().code, "xtdb/zero-leg-read");
}

#[test]
fn selecting_a_list_with_an_unreadable_child_preserves_its_read_error() {
    let logical = LogicalType::list(LogicalType::Nothing);
    let field = Arc::new(rtdb_columnar::to_field("v", &logical).unwrap());
    let values = rtdb_columnar::build_array(&logical, &[Value::list(vec![Value::Undefined])]).unwrap();
    let selected = SelectedColumn {
        sources: Arc::new(vec![Some((values, Arc::clone(&field), Some(logical.clone()), None))]),
        cells: Arc::new(vec![(0, 0)]),
    };
    let memory = MemoryBudget::new(1 << 20);

    assert!(selected.null_test(false).is_none());
    let column = Column::Selected(selected);
    assert!(relayout_output(&column, &logical, &field, &memory).unwrap().is_none());
    assert_eq!(column.value(0).unwrap_err().code, "xtdb/zero-leg-read");
}

fn can_interleave_nested(array: &dyn Array) -> bool {
    can_interleave_layout(array, false)
}

fn can_interleave_layout(array: &dyn Array, include_fixed_lists_and_maps: bool) -> bool {
    if flat(array.data_type()) { return true; }
    if let (DataType::Union(legs, arrow_schema::UnionMode::Dense), Some(union)) =
        (array.data_type(), array.as_any().downcast_ref::<UnionArray>()) {
        return legs.iter().all(|(id, _)| can_interleave_layout(union.child(id).as_ref(), include_fixed_lists_and_maps));
    }
    if let Some(structs) = array.as_any().downcast_ref::<StructArray>() {
        return structs.columns().iter().all(|child| can_interleave_layout(child.as_ref(), include_fixed_lists_and_maps));
    }
    if let (DataType::List(field), Some(lists)) = (array.data_type(), array.as_any().downcast_ref::<ListArray>()) {
        return (field.is_nullable() || !lists.values().is_nullable()) && can_interleave_layout(lists.values().as_ref(), include_fixed_lists_and_maps);
    }
    if include_fixed_lists_and_maps {
        if let Some(lists) = array.as_any().downcast_ref::<FixedSizeListArray>() {
            return can_interleave_layout(lists.values().as_ref(), true);
        }
        if let Some(maps) = array.as_any().downcast_ref::<MapArray>() {
            return can_interleave_layout(maps.entries(), true);
        }
    }
    false
}

pub(crate) fn flat(data_type: &DataType) -> bool {
    scalar_layout(data_type) || matches!(data_type, DataType::Union(fields, arrow_schema::UnionMode::Dense) if fields.iter().all(|(_, field)| scalar_layout(field.data_type())))
}

/// Layouts with no child arrays, whose cells interleave without any validation that could reject them.
fn scalar_layout(data_type: &DataType) -> bool {
    data_type.is_primitive() || matches!(data_type, DataType::Boolean | DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) | DataType::Null)
}

fn size_error() -> rtdb_types::Anomaly { invalid("column-size", "Column page exceeds addressable memory") }

pub(crate) fn fragment_column(array: ArrayRef, field: Arc<ArrowField>, logical: Option<LogicalType>,
    memory: &Arc<MemoryBudget>, metadata: &mut MemoryReservation) -> Result<Column> {
    if !nested_layout(array.data_type()) {
        metadata.grow_to(metadata.bytes().checked_add(array.get_array_memory_size()).ok_or_else(size_error)?)?;
        return Ok(Column::Arrow { array, field, logical });
    }
    let bytes = array.len().checked_mul(std::mem::size_of::<(usize, usize)>()).and_then(|bytes| bytes.checked_add(512))
        .and_then(|bytes| bytes.checked_add(field.size())).ok_or_else(size_error)?;
    metadata.grow_to(metadata.bytes().checked_add(bytes).ok_or_else(size_error)?)?;
    let size = array.get_array_memory_size().checked_add(field.size()).ok_or_else(size_error)?;
    let reservation = memory.reserve(size)?;
    let cells = (0..array.len()).map(|row| (0, row)).collect();
    Ok(Column::Selected(SelectedColumn {
        sources: Arc::new(vec![Some((array, field, logical, Some(reservation)))]),
        cells: Arc::new(cells),
    }))
}

pub(crate) fn nested_layout(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Struct(_) | DataType::List(_) | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _) | DataType::Union(_, _) | DataType::RunEndEncoded(_, _))
}

pub(crate) fn compact_arrow_rows(array: &dyn Array, field: &ArrowField, rows: impl Iterator<Item = usize> + Clone,
    memory: &Arc<MemoryBudget>) -> Result<(ArrayRef, Arc<MemoryReservation>)> {
    let base = field.size().checked_mul(8).and_then(|size| size.checked_add(4096)).ok_or_else(size_error)?;
    let bound = rows.clone().try_fold(base, |bytes, row| {
        rtdb_columnar::estimate_copy_cell_bytes(field, array, row)?.checked_mul(4)
            .and_then(|size| bytes.checked_add(size)).ok_or_else(size_error)
    })?;
    let mut reservation = memory.reserve(bound)?;
    let picks = rows.map(|row| (0, row)).collect::<Vec<_>>();
    let output = copy_selected_cells(&[array], &picks)?;
    let lease = Arc::get_mut(&mut reservation).expect("A compact column owns its reservation");
    lease.shrink_to(output.get_array_memory_size())?;
    Ok((output, reservation))
}

/// The scalar a column's cells read as, where its logical type is one scalar, nullable or not: a nullable column's
/// cells read as its scalar's or as null, so the kernels treat the two alike.
fn plain_scalar(logical: &LogicalType) -> Option<&rtdb_types::ScalarType> {
    match logical {
        LogicalType::Scalar(scalar) => Some(scalar),
        LogicalType::Maybe(inner) => match inner.as_ref() { LogicalType::Scalar(scalar) => Some(scalar), _ => None },
        _ => None,
    }
}

/// Scalars whose cells survive a copy through their own Arrow layout unchanged, with no admission or validation on the way.
fn copyable(scalar: &rtdb_types::ScalarType) -> bool {
    use rtdb_types::ScalarType as S;
    matches!(scalar, S::Bool | S::I8 | S::I16 | S::I32 | S::I64 | S::F32 | S::F64 | S::Utf8 | S::Binary | S::Uuid | S::U8 | S::U16 | S::U32 | S::U64 | S::FixedSizeBinary(_) | S::Timestamp { .. })
}

/// The column's array where it already holds exactly what copying its cells into `field`, the layout of `target`,
/// would build; `None` where the cells must be copied.
pub(crate) fn conform(column: &Column, target: &LogicalType, field: &ArrowField) -> Option<ArrayRef> {
    let Column::Arrow { array, logical: Some(logical), .. } = column else { return None };
    conform_array(array, logical, target, field)
}

pub(crate) fn relayout_output(column: &Column, target: &LogicalType, field: &ArrowField, memory: &Arc<MemoryBudget>)
    -> Result<Option<(ArrayRef, Arc<MemoryReservation>)>> {
    if let Column::Selected(column) = column {
        return column.relayout(target, field, memory);
    }
    let Column::Arrow { array, field: source, logical: Some(logical) } = column else { return Ok(None) };
    if !stable_read_layout(logical, source) || logical.join(target) != *target { return Ok(None); }
    let bound = rtdb_columnar::arrow_relayout_bound(array.as_ref(), source, field)?;
    let Ok(scratch) = memory.reserve(bound) else { return Ok(None) };
    let Ok((physical, array)) = rtdb_columnar::relayout_arrow_column(Arc::clone(array), source, field) else { return Ok(None) };
    if physical.data_type() != field.data_type() || (!target.nullable() && array.logical_null_count() != 0) {
        return Ok(None);
    }
    Ok(Some((array, scratch)))
}

impl SelectedColumn {
    fn relayout(&self, target: &LogicalType, field: &ArrowField, memory: &Arc<MemoryBudget>)
        -> Result<Option<(ArrayRef, Arc<MemoryReservation>)>> {
        let base = self.cells.len().checked_mul(64).and_then(|bytes| self.sources.len().checked_mul(128).and_then(|extra| bytes.checked_add(extra)))
            .and_then(|bytes| field.size().checked_mul(8).and_then(|extra| bytes.checked_add(extra))).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
        let Ok(mut scratch) = memory.reserve(base) else { return Ok(None); };
        let mut used = vec![false; self.sources.len()];
        for &(source, _) in &*self.cells { used[source] = true; }
        let mut positions = vec![0; self.sources.len()];
        let mut arrays = Vec::new();
        let null = ArrowField::new(field.name(), DataType::Null, true);
        for (position, source) in self.sources.iter().enumerate().filter(|(position, _)| used[*position]) {
            let (source, array, logical) = match source {
                Some((array, source, Some(logical), _)) if *logical != LogicalType::Nothing && stable_read_layout(logical, source) => {
                    (source.as_ref(), Arc::clone(array), logical)
                }
                Some(_) => return Ok(None),
                None => (&null, Arc::new(arrow_array::NullArray::new(1)) as ArrayRef, &LogicalType::Null),
            };
            let array = if source.data_type() == field.data_type() && logical.clone().maybe() == target.clone().maybe() {
                array
            } else {
                if logical.join(target) != *target { return Ok(None); }
                let bound = rtdb_columnar::arrow_relayout_bound(array.as_ref(), source, field)?;
                let lease = Arc::get_mut(&mut scratch).expect("A selection owns its scratch");
                if lease.grow_to(lease.bytes().checked_add(bound).ok_or_else(size_error)?).is_err() { return Ok(None); }
                let Ok((physical, array)) = rtdb_columnar::relayout_arrow_column(array, source, field) else { return Ok(None); };
                if physical.data_type() != field.data_type() { return Ok(None); }
                array
            };
            if !can_interleave_layout(array.as_ref(), true) { return Ok(None); }
            positions[position] = arrays.len();
            arrays.push(array);
        }
        if arrays.is_empty() { return Ok(None); }
        let picks = self.cells.iter().map(|&(source, row)| (positions[source], if self.sources[source].is_some() { row } else { 0 })).collect::<Vec<_>>();
        let lease = Arc::get_mut(&mut scratch).expect("A selection owns its scratch");
        let bound = picks.iter().try_fold(lease.bytes(), |bytes, &(source, row)| {
            rtdb_columnar::estimate_copy_cell_bytes(field, arrays[source].as_ref(), row)?.checked_mul(4)
                .and_then(|size| bytes.checked_add(size)).ok_or_else(size_error)
        })?;
        if lease.grow_to(bound).is_err() { return Ok(None); }
        let arrays = arrays.iter().map(|array| array.as_ref()).collect::<Vec<_>>();
        let Ok(array) = copy_selected_cells(&arrays, &picks) else { return Ok(None); };
        if !target.nullable() && array.logical_null_count() != 0 { return Ok(None); }
        Ok(Some((array, scratch)))
    }
}

fn stable_read_layout(logical: &LogicalType, field: &ArrowField) -> bool {
    stable_values(logical) && rtdb_columnar::execution_type(logical) == *logical && rtdb_columnar::from_field(field).is_ok_and(|ty| ty == *logical)
}

fn readable_list_type(logical: &LogicalType, field: &ArrowField) -> bool {
    fn readable(field: &ArrowField) -> bool {
        match field.data_type() {
            DataType::Null => field.is_nullable(),
            DataType::Union(..) => false,
            DataType::Struct(fields) => fields.iter().all(|child| readable(child)),
            DataType::List(child) | DataType::FixedSizeList(child, _) | DataType::Map(child, _) => readable(child),
            _ => true,
        }
    }
    let logical = match logical { LogicalType::Maybe(inner) => inner.as_ref(), ty => ty };
    match (logical, field.data_type()) {
        (LogicalType::Null, DataType::Null) => true,
        (LogicalType::List { kind: ListKind::List | ListKind::FixedSizeList(_) | ListKind::Set, .. },
         DataType::List(child) | DataType::FixedSizeList(child, _)) => readable(child),
        _ => false,
    }
}

fn stable_values(logical: &LogicalType) -> bool {
    match logical {
        LogicalType::Null | LogicalType::Nothing => true,
        LogicalType::Scalar(scalar) => copyable(scalar) || matches!(scalar,
            rtdb_types::ScalarType::F16 | rtdb_types::ScalarType::DateDay | rtdb_types::ScalarType::DateMilli
            | rtdb_types::ScalarType::Decimal { .. } | rtdb_types::ScalarType::Time { .. }
            | rtdb_types::ScalarType::Duration(_) | rtdb_types::ScalarType::Interval(_)
            | rtdb_types::ScalarType::IntervalMonthDayMicro | rtdb_types::ScalarType::Keyword
            | rtdb_types::ScalarType::Transit | rtdb_types::ScalarType::Oid
            | rtdb_types::ScalarType::RegClass | rtdb_types::ScalarType::RegProc),
        LogicalType::Maybe(ty) => stable_values(ty),
        LogicalType::Struct(fields) => fields.values().all(stable_values),
        LogicalType::List { kind: ListKind::List | ListKind::FixedSizeList(_) | ListKind::Set | ListKind::Map { .. }, element } => {
            **element != LogicalType::Nothing && stable_values(element)
        }
        LogicalType::Union(legs) => legs.iter().all(stable_values),
        _ => false,
    }
}

fn conform_array(array: &ArrayRef, logical: &LogicalType, target: &LogicalType, field: &ArrowField) -> Option<ArrayRef> {
    if !field.is_nullable() && array.null_count() != 0 { return None; }
    let strip = |logical: &LogicalType| match logical { LogicalType::Maybe(inner) => inner.as_ref().clone(), other => other.clone() };
    match (strip(logical), strip(target)) {
        (LogicalType::Null | LogicalType::Nothing, LogicalType::Null | LogicalType::Nothing)
            if array.data_type() == &DataType::Null && field.data_type() == &DataType::Null => Some(Arc::clone(array)),
        (LogicalType::Scalar(scalar), LogicalType::Scalar(expected)) => {
            let kept = copyable(&scalar) || scalar == rtdb_types::ScalarType::DateDay;
            (scalar == expected && kept && array.data_type() == field.data_type()).then(|| Arc::clone(array))
        }
        (LogicalType::Union(legs), LogicalType::Union(expected)) => {
            let kept = legs.iter().all(|leg| match leg {
                LogicalType::Scalar(scalar) => copyable(scalar) || matches!(scalar,
            rtdb_types::ScalarType::F16 | rtdb_types::ScalarType::DateDay | rtdb_types::ScalarType::DateMilli
            | rtdb_types::ScalarType::Decimal { .. } | rtdb_types::ScalarType::Time { .. }
            | rtdb_types::ScalarType::Duration(_) | rtdb_types::ScalarType::Interval(_)
            | rtdb_types::ScalarType::IntervalMonthDayMicro | rtdb_types::ScalarType::Keyword
            | rtdb_types::ScalarType::Transit | rtdb_types::ScalarType::Oid
            | rtdb_types::ScalarType::RegClass | rtdb_types::ScalarType::RegProc),
                LogicalType::Null => true,
                _ => stable_values(leg) && rtdb_columnar::execution_type(leg) == *leg,
            });
            (kept && legs == expected && array.data_type() == field.data_type()).then(|| Arc::clone(array))
        }
        // A copied record's fields take the order of the result's, whatever order the cells came in.
        (LogicalType::Struct(children), LogicalType::Struct(expected)) => {
            let (DataType::Struct(fields), Some(structs)) = (field.data_type(), array.as_any().downcast_ref::<StructArray>()) else { return None };
            if structs.num_columns() != fields.len() || children.len() != fields.len() || expected.len() != fields.len() { return None; }
            let mut columns = Vec::with_capacity(fields.len());
            for target_field in fields.iter() {
                let index = structs.fields().iter().position(|source| source.name() == target_field.name())?;
                columns.push(conform_array(structs.column(index), children.get(target_field.name())?, expected.get(target_field.name())?, target_field)?);
            }
            StructArray::try_new_with_length(fields.clone(), columns, structs.nulls().cloned(), structs.len())
                .ok().map(|array| Arc::new(array) as ArrayRef)
        }
        (LogicalType::List { kind: ListKind::List, element }, LogicalType::List { kind: ListKind::List, element: expected }) => {
            let (DataType::List(target_item), Some(lists)) = (field.data_type(), array.as_any().downcast_ref::<ListArray>()) else { return None; };
            let values = conform_array(lists.values(), &element, &expected, target_item)?;
            ListArray::try_new(Arc::clone(target_item), lists.offsets().clone(), values, lists.nulls().cloned())
                .ok().map(|array| Arc::new(array) as ArrayRef)
        }
        (LogicalType::List { kind: ListKind::FixedSizeList(width), element }, LogicalType::List { kind: ListKind::FixedSizeList(expected_width), element: expected })
            if width == expected_width => {
            let (DataType::FixedSizeList(target_item, target_width), Some(lists)) = (field.data_type(), array.as_any().downcast_ref::<FixedSizeListArray>()) else { return None; };
            if width == 0 || *target_width != width || lists.value_length() != width { return None; }
            let values = conform_array(lists.values(), &element, &expected, target_item)?;
            FixedSizeListArray::try_new(Arc::clone(target_item), width, values, lists.nulls().cloned())
                .ok().map(|array| Arc::new(array) as ArrayRef)
        }
        _ => None,
    }
}

/// A batch source: a scan, or an operator that keeps its input's columns.
#[async_trait::async_trait]
pub(crate) trait BatchCursor: Send {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>>;
    fn skip_complete_pages(&mut self, _rows: u64) -> Result<u64> { Ok(0) }
    fn trace_attributes(&self, _span: &mut dyn rtdb_plan::TraceSpan) {}
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { None }
}
pub(crate) type BoxBatchCursor = Box<dyn BatchCursor>;

pub(crate) fn single_batch(batch: Option<ColumnBatch>, context: Arc<crate::QueryContext>) -> BoxBatchCursor {
    Box::new(SingleBatch { batch, context })
}

struct SingleBatch { batch: Option<ColumnBatch>, context: Arc<crate::QueryContext> }

#[async_trait::async_trait]
impl BatchCursor for SingleBatch {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        self.context.check_cancelled()?;
        Ok(self.batch.take())
    }
}

pub(crate) async fn poll_nested_batch(input: &mut dyn BatchCursor) -> Result<Option<ColumnBatch>> {
    let mut pull = input.next_batch();
    std::future::poll_fn(|context| stacker::maybe_grow(512 * 1024, 2 * 1024 * 1024, || pull.as_mut().poll(context))).await
}

#[async_trait::async_trait]
impl BatchCursor for crate::scan::ScanCursor {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> { crate::scan::ScanCursor::next_batch(self).await }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { crate::pages::PageCursor::trace_attributes(self, span) }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { crate::pages::PageCursor::scan_counts(self) }
}

pub(crate) fn retain_batch_state(input: BoxBatchCursor, reservation: Arc<MemoryReservation>) -> BoxBatchCursor { Box::new(RetainedBatchState { input, _reservation: reservation }) }
/// As `pages::retain_page_state`, spending cooperative budget per batch.
struct RetainedBatchState { input: BoxBatchCursor, _reservation: Arc<MemoryReservation> }
#[async_trait::async_trait]
impl BatchCursor for RetainedBatchState {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        tokio::task::consume_budget().await;
        self.input.next_batch().await
    }
    fn skip_complete_pages(&mut self, rows: u64) -> Result<u64> { self.input.skip_complete_pages(rows) }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { self.input.trace_attributes(span) }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
}

pub(crate) fn coalesce_pages(input: BoxBatchCursor, context: Arc<crate::QueryContext>) -> BoxBatchCursor {
    Box::new(CoalescingBatches { input, context, seen: 0 })
}

struct CoalescingBatches { input: BoxBatchCursor, context: Arc<crate::QueryContext>, seen: usize }

#[async_trait::async_trait]
impl BatchCursor for CoalescingBatches {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        let mut sources = Vec::new();
        let mut spans = Vec::new();
        let mut rows = 0usize;
        let memory = &self.context.memory;
        let mut inventory = None;
        loop {
            self.context.check_cancelled()?;
            let Some(batch) = poll_nested_batch(self.input.as_mut()).await? else { break; };
            if self.seen < 100 {
                self.seen = self.seen.saturating_add(batch.len);
                return Ok(Some(batch));
            }
            if batch.len >= 100 && sources.is_empty() { return Ok(Some(batch)); }
            if batch.len == 0 { continue; }
            let bytes = (sources.len() + 1).checked_mul(128).ok_or_else(size_error)?;
            let lease = match &mut inventory {
                Some(reservation) => reservation,
                slot => slot.insert(memory.reserve(0)?),
            };
            Arc::get_mut(lease).expect("Coalescer owns its batch inventory").grow_to(bytes)?;
            spans.push(0..batch.len);
            rows = rows.checked_add(batch.len).ok_or_else(size_error)?;
            sources.push(Arc::new(batch));
            if rows >= 100 { break; }
        }
        ColumnBatch::take_ranges(sources, &spans, rows, memory)
    }

    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { self.input.trace_attributes(span); }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
}

pub(crate) fn batches_from_pages(pages: crate::pages::BoxPageCursor, width: usize, memory: Arc<MemoryBudget>) -> BoxBatchCursor {
    Box::new(BatchesFromPages { pages, width, memory })
}
struct BatchesFromPages { pages: crate::pages::BoxPageCursor, width: usize, memory: Arc<MemoryBudget> }

#[async_trait::async_trait]
impl BatchCursor for BatchesFromPages {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        self.pages.next_page().await?.map(|page| {
            if page.len() == 0 { ColumnBatch::empty_with_width(self.width, &self.memory) }
            else { ColumnBatch::from_page(&page, &self.memory) }
        }).transpose()
    }
}

/// Materialises a batch source's rows for row operators.
pub(crate) struct BatchRows { input: BoxBatchCursor, memory: Arc<MemoryBudget>, admit_dates: bool }
impl BatchRows {
    pub fn new(input: BoxBatchCursor, memory: Arc<MemoryBudget>) -> Self { Self { input, memory, admit_dates: false } }
    /// Admits each cell's dates as the row projection the batches stand in for would have.
    pub fn admitting_dates(self, admit_dates: bool) -> Self { Self { admit_dates, ..self } }
}
#[async_trait::async_trait]
impl crate::pages::PageCursor for BatchRows {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        let Some(batch) = self.input.next_batch().await? else { return Ok(None) };
        batch.rows_admitting(&self.memory, self.admit_dates).map(Some)
    }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { self.input.trace_attributes(span) }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
}

impl ColumnBatch {
    pub(crate) fn slice(mut self, start: usize, count: usize, memory: &Arc<MemoryBudget>) -> Result<Self> {
        if count == self.len { return Ok(self); }
        let end = start + count;
        let scratch = self.columns.iter().filter(|column| matches!(column, Column::Selected(_))).count()
            .checked_mul(count).and_then(|cells| cells.checked_mul(std::mem::size_of::<(usize, usize)>())).ok_or_else(size_error)?;
        let _selection = memory.reserve(scratch)?;
        for column in &mut self.columns {
            match column {
                Column::Arrow { array, .. } => *array = array.slice(start, count),
                Column::Selected(column) => column.cells = Arc::new(column.cells[start..end].to_vec()),
                Column::Values(values) => { values.truncate(end); values.drain(..start); }
            }
        }
        self.len = count;
        Ok(self)
    }

    /// Keeps the rows `mask` selects, column by column.
    pub(crate) fn select(self, mask: &[bool], memory: &Arc<MemoryBudget>) -> Result<Self> {
        let selected = mask.iter().filter(|keep| **keep).count();
        if selected == self.len { return Ok(self); }
        // A selection holds a subset of each column's cells, so it retains no more than the page it is taken from.
        let reservation = memory.reserve(self.charged().saturating_add(mask.len()))?;
        let predicate = arrow_array::BooleanArray::from(mask.to_vec());
        let columns = self.columns.into_iter().map(|column| Ok(match column {
            Column::Arrow { array, field, logical } => Column::Arrow { array: arrow_select::filter::filter(array.as_ref(), &predicate).map_err(|error| invalid("column-filter", error))?, field, logical },
            Column::Selected(column) => Column::Selected(column.select(mask, selected)),
            Column::Values(values) => Column::Values(values.into_iter().zip(mask).filter_map(|(value, keep)| keep.then_some(value)).collect()),
        })).collect::<Result<Vec<_>>>()?;
        Self::settled(columns, selected, reservation)
    }
}

/// Filters batches, evaluating the predicate over whole columns where its shape and the columns' types allow,
/// and row by row, exactly as the row filter does, for any batch where they don't.
pub(crate) struct BatchFilter { input: BoxBatchCursor, schema: rtdb_plan::Schema, stages: Vec<rtdb_plan::Expr>, execution: crate::context::Execution }
impl BatchFilter {
    pub fn new(input: BoxBatchCursor, schema: rtdb_plan::Schema, predicate: rtdb_plan::Expr, execution: crate::context::Execution) -> Self {
        Self::with_stages(input, schema, vec![predicate], execution)
    }
    pub fn with_stages(input: BoxBatchCursor, schema: rtdb_plan::Schema, stages: Vec<rtdb_plan::Expr>, execution: crate::context::Execution) -> Self {
        Self { input, schema, stages, execution }
    }
}
#[async_trait::async_trait]
impl BatchCursor for BatchFilter {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        'batches: while let Some(mut batch) = poll_nested_batch(self.input.as_mut()).await? {
            for predicate in &self.stages {
                let mask = predicate_mask(&batch, predicate, &self.schema, &self.execution).await?;
                if !mask.iter().any(|keep| *keep) { continue 'batches; }
                batch = batch.select(&mask, &self.execution.context.memory)?;
            }
            return Ok(Some(batch));
        }
        Ok(None)
    }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { self.input.trace_attributes(span) }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
}

/// Computes selection in place so an ordered seek can keep row offsets into the original batch.
pub(crate) async fn predicate_mask(
    batch: &ColumnBatch, predicate: &rtdb_plan::Expr, schema: &rtdb_plan::Schema, execution: &crate::context::Execution,
) -> Result<Vec<bool>> {
    execution.context.check_cancelled()?;
    if search::shaped(predicate)
        && let Some(Column::Arrow { array, .. }) = search::column(predicate, schema, batch, execution.parameters.evaluator(), &execution.context)?
        && let Some(values) = array.as_any().downcast_ref::<arrow_array::BooleanArray>()
    {
        return Ok(values.iter().map(|value| value == Some(true)).collect());
    }
    let truth = kernel::truth(predicate, schema, batch, execution.parameters.evaluator())
        .or_else(|| crate::vector::truth(predicate, schema, batch, execution.parameters.evaluator()));
    if let Some(truth) = truth {
        return Ok(truth.into_iter().map(|value| value == Some(true)).collect());
    }
    let metadata = execution.scope()?;
    let invocation = execution.prepare_expression(predicate, schema, &metadata)?;
    let scope = execution.scope()?;
    let mut mask = Vec::with_capacity(batch.len);
    for row in 0..batch.len {
        let input = BatchRow { batch, row, admit_dates: false };
        let value = execution.evaluate_invocation_row(
            &scope, &invocation, rtdb_expr::ExpressionRow::Borrowed(&input),
        ).await?;
        mask.push(rtdb_expr::is_true(&value)?);
        scope.reset()?;
    }
    Ok(mask)
}

pub(crate) struct BatchRow<'a> { pub batch: &'a ColumnBatch, pub row: usize, pub admit_dates: bool }

impl Column {
    pub(crate) fn expression_cell(&self, row: usize) -> Option<rtdb_columnar::ArrowCell<'_>> {
        let (array, field, logical, row) = self.arrow_cell(row)??;
        if logical.is_some_and(|logical| rtdb_columnar::from_field(field).ok().as_ref() != Some(logical)) {
            return None;
        }
        let cell = rtdb_columnar::ArrowCell { field, array, row };
        if cell.is_undefined().ok()? { return None; }
        Some(cell)
    }

    pub(crate) fn expression_value(&self, row: usize, admit_dates: bool, allocation: Option<&rtdb_expr::EvaluationScope>) -> Result<Value> {
        if let Some(scope) = allocation { scope.reserve(self.bound(row)?)?; }
        let mut value = self.value(row)?;
        if admit_dates { value.admit_expression_dates()?; }
        Ok(value)
    }
}

impl rtdb_expr::RowInput for BatchRow<'_> {
    fn arrow_cell(&self, index: usize) -> Option<rtdb_columnar::ArrowCell<'_>> {
        self.batch.columns.get(index)?.expression_cell(self.row)
    }

    fn value(&self, index: usize, allocation: Option<&rtdb_expr::EvaluationScope>) -> Result<Option<Value>> {
        self.batch.columns.get(index).map(|column| column.expression_value(self.row, self.admit_dates, allocation)).transpose()
    }
}

/// Three-valued predicate evaluation over a batch's columns, for the predicate shapes and cell types whose
/// semantics are exactly those of the scalar evaluator; `None` sends the batch to it instead.
mod kernel {
    use arrow_array::{Array, BooleanArray, FixedSizeBinaryArray, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType, TimeUnit as ArrowUnit};
    use rtdb_plan::{BinaryOp, Expr, Schema, UnaryOp};
    use rtdb_types::{ScalarType, TimeUnit, Value};
    use super::{Column, ColumnBatch};

    type Truth = Vec<Option<bool>>;

    /// A comparable cell; integers and floats compare only where the conversion between them is exact.
    #[derive(Clone, Copy, PartialEq, PartialOrd)]
    enum Cell<'a> { Int(i64), Float(f64), Text(&'a str), Bool(bool), Micros(i64, Option<&'a str>),
        /// A UUID as the big-endian integer of its bytes, which orders as the bytes do.
        Uuid(u128) }

    enum Cells<'a> { Uuid(&'a FixedSizeBinaryArray), Int(&'a Int64Array), Int32(&'a Int32Array), Int16(&'a Int16Array), Int8(&'a Int8Array), Float(&'a Float64Array), Text(&'a StringArray), Bool(&'a BooleanArray), Micros(&'a TimestampMicrosecondArray, Option<&'a str>), Values(&'a [Value]), Owned(Vec<Option<Cell<'static>>>),
        /// A dense union whose legs are each a plain cell type, read through the leg each cell's type id names.
        Union { array: &'a arrow_array::UnionArray, legs: Vec<Option<Cells<'a>>> } }

    impl<'a> Cells<'a> {
        fn of(column: &'a Column) -> Option<Self> {
            match column {
                Column::Values(values) => Some(Self::Values(values)),
                Column::Selected(_) => None,
                Column::Arrow { array, field, logical } => {
                    let scalar = logical.as_ref().and_then(super::plain_scalar);
                    let plain = |expected: ScalarType| match logical { Some(_) => scalar == Some(&expected), None => field.metadata().is_empty() };
                    let any = array.as_any();
                    match array.data_type() {
                        DataType::Int64 if plain(ScalarType::I64) => any.downcast_ref().map(Self::Int),
                        DataType::Int32 if plain(ScalarType::I32) => any.downcast_ref().map(Self::Int32),
                        DataType::Int16 if plain(ScalarType::I16) => any.downcast_ref().map(Self::Int16),
                        DataType::Int8 if plain(ScalarType::I8) => any.downcast_ref().map(Self::Int8),
                        DataType::Float64 if plain(ScalarType::F64) => any.downcast_ref().map(Self::Float),
                        DataType::Utf8 if plain(ScalarType::Utf8) => any.downcast_ref().map(Self::Text),
                        DataType::Boolean if plain(ScalarType::Bool) => any.downcast_ref().map(Self::Bool),
                        // Sixteen bytes are a UUID only where the column's logical type says so.
                        DataType::FixedSizeBinary(16) if scalar == Some(&ScalarType::Uuid) => any.downcast_ref().map(Self::Uuid),
                        // A cell reads its zone from the column's logical type, or from the layout where it has none.
                        DataType::Timestamp(ArrowUnit::Microsecond, zone) => {
                            let zone = match (logical, scalar) {
                                (Some(_), Some(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone })) => timezone.as_deref(),
                                (None, _) if field.metadata().is_empty() => zone.as_deref(),
                                _ => return None,
                            };
                            any.downcast_ref().map(|array| Self::Micros(array, zone))
                        }
                        DataType::Union(fields, arrow_schema::UnionMode::Dense) => {
                            let array: &arrow_array::UnionArray = any.downcast_ref()?;
                            let mut legs: Vec<Option<Self>> = Vec::new();
                            for (id, field) in fields.iter() {
                                if id < 0 || !field.metadata().is_empty() { return None; }
                                let child = array.child(id);
                                let leg_any = child.as_any();
                                let cells = match child.data_type() {
                                    DataType::Int64 => Self::Int(leg_any.downcast_ref()?),
                                    DataType::Int32 => Self::Int32(leg_any.downcast_ref()?),
                                    DataType::Int16 => Self::Int16(leg_any.downcast_ref()?),
                                    DataType::Int8 => Self::Int8(leg_any.downcast_ref()?),
                                    DataType::Float64 => Self::Float(leg_any.downcast_ref()?),
                                    DataType::Utf8 => Self::Text(leg_any.downcast_ref()?),
                                    DataType::Boolean => Self::Bool(leg_any.downcast_ref()?),
                                    _ => return None,
                                };
                                let id = id as usize;
                                if legs.len() <= id { legs.resize_with(id + 1, || None); }
                                legs[id] = Some(cells);
                            }
                            Some(Self::Union { array, legs })
                        }
                        _ => None,
                    }
                }
            }
        }
        /// `Err` where a cell has a type the kernel doesn't evaluate.
        fn get(&self, row: usize) -> Result<Option<Cell<'a>>, ()> {
            fn valid<A: Array>(array: &A, row: usize) -> bool { array.is_valid(row) }
            Ok(match self {
                Self::Int(array) => valid(*array, row).then(|| Cell::Int(array.value(row))),
                Self::Int32(array) => valid(*array, row).then(|| Cell::Int(i64::from(array.value(row)))),
                Self::Int16(array) => valid(*array, row).then(|| Cell::Int(i64::from(array.value(row)))),
                Self::Int8(array) => valid(*array, row).then(|| Cell::Int(i64::from(array.value(row)))),
                Self::Owned(cells) => cells[row],
                Self::Float(array) => valid(*array, row).then(|| Cell::Float(array.value(row))),
                Self::Text(array) => valid(*array, row).then(|| Cell::Text(array.value(row))),
                Self::Bool(array) => valid(*array, row).then(|| Cell::Bool(array.value(row))),
                Self::Micros(array, zone) => valid(*array, row).then(|| Cell::Micros(array.value(row), *zone)),
                Self::Uuid(array) => match valid(*array, row).then(|| array.value(row).try_into()) {
                    Some(bytes) => Some(Cell::Uuid(u128::from_be_bytes(bytes.map_err(|_| ())?))),
                    None => None,
                },
                Self::Values(values) => scalar(&values[row])?,
                Self::Union { array, legs } => {
                    let leg = usize::try_from(array.type_id(row)).ok().and_then(|id| legs.get(id)).and_then(Option::as_ref).ok_or(())?;
                    leg.get(array.value_offset(row))?
                }
            })
        }
    }

    /// A computed numeric cell, which borrows nothing.
    fn owned(cell: Cell<'_>) -> Option<Cell<'static>> {
        match cell {
            Cell::Int(value) => Some(Cell::Int(value)), Cell::Float(value) => Some(Cell::Float(value)), Cell::Bool(value) => Some(Cell::Bool(value)), Cell::Uuid(value) => Some(Cell::Uuid(value)),
            Cell::Micros(value, None) => Some(Cell::Micros(value, None)), Cell::Micros(value, Some("UTC")) => Some(Cell::Micros(value, Some("UTC"))),
            Cell::Micros(_, Some(_)) | Cell::Text(_) => None,
        }
    }
    fn scalar(value: &Value) -> Result<Option<Cell<'_>>, ()> {
        Ok(match value {
            Value::Null => None,
            Value::I8(value) => Some(Cell::Int(i64::from(*value))),
            Value::I16(value) => Some(Cell::Int(i64::from(*value))),
            Value::I32(value) => Some(Cell::Int(i64::from(*value))),
            Value::I64(value) => Some(Cell::Int(*value)),
            Value::F32(value) => Some(Cell::Float(f64::from(*value))),
            Value::F64(value) => Some(Cell::Float(*value)),
            Value::Utf8(value) => Some(Cell::Text(value)),
            Value::Bool(value) => Some(Cell::Bool(*value)),
            Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone } => Some(Cell::Micros(*value, timezone.as_deref())),
            Value::Uuid(bytes) => Some(Cell::Uuid(u128::from_be_bytes(*bytes))),
            _ => return Err(()),
        })
    }

    const EXACT: i64 = 1 << 53;
    /// Orders two cells as the scalar comparison does, or `Err` where the pair is outside the kernel.
    fn order(left: Cell<'_>, right: Cell<'_>, equality_only: bool) -> Result<std::cmp::Ordering, ()> {
        use std::cmp::Ordering;
        let float = |value: f64| if value.is_nan() { Err(()) } else { Ok(value) };
        // Equality is IEEE (`-0.0 = 0.0`), ordering is total (`-0.0 < 0.0`), as in the scalar evaluator.
        let floats = |a: f64, b: f64| if equality_only { a.partial_cmp(&b).ok_or(()) } else { Ok(a.total_cmp(&b)) };
        match (left, right) {
            (Cell::Int(a), Cell::Int(b)) => Ok(a.cmp(&b)),
            (Cell::Micros(a, zoned), Cell::Micros(b, other)) if zoned.is_some() == other.is_some() => Ok(a.cmp(&b)),
            (Cell::Float(a), Cell::Float(b)) => floats(float(a)?, float(b)?),
            (Cell::Int(a), Cell::Float(b)) if a.unsigned_abs() < EXACT as u64 => floats(a as f64, float(b)?),
            (Cell::Float(a), Cell::Int(b)) if b.unsigned_abs() < EXACT as u64 => floats(float(a)?, b as f64),
            (Cell::Text(a), Cell::Text(b)) if equality_only || (a.is_ascii() && b.is_ascii()) => Ok(a.cmp(b)),
            (Cell::Bool(a), Cell::Bool(b)) if equality_only => Ok(if a == b { Ordering::Equal } else { Ordering::Less }),
            (Cell::Uuid(a), Cell::Uuid(b)) => Ok(a.cmp(&b)),
            _ => Err(()),
        }
    }

    enum Operand<'a> { Cells(Cells<'a>), Constant(Option<Cell<'a>>) }
    fn can_read_owned_cell(data_type: &DataType) -> bool {
        match data_type {
            DataType::Union(fields, arrow_schema::UnionMode::Dense) => fields.iter().all(|(_, field)| can_read_owned_cell(field.data_type())),
            DataType::RunEndEncoded(_, field) => can_read_owned_cell(field.data_type()),
            DataType::Null | DataType::Boolean | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 | DataType::Float32 | DataType::Float64
            | DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => true,
            _ => false,
        }
    }

    impl<'a> Operand<'a> {
        fn of(expr: &'a Expr, schema: &Schema, batch: &'a ColumnBatch, parameters: rtdb_expr::Parameters<'a>) -> Option<Self> {
            match expr {
                Expr::Column { id, .. } => {
                    let column = &batch.columns[schema.position(*id)?];
                    match Cells::of(column) {
                        Some(cells) => Some(Self::Cells(cells)),
                        None if matches!(column, Column::Selected(_)) => None,
                        None if matches!(column, Column::Arrow { array, .. } if !can_read_owned_cell(array.data_type())) => None,
                        // A column without a direct cell reader, a union of integer legs say, is read once into owned cells.
                        None => (0..batch.len).map(|row| {
                            let value = column.value(row).ok()?;
                            match scalar(&value).ok()? { None => Some(None), Some(cell) => owned(cell).map(Some) }
                        }).collect::<Option<Vec<_>>>().map(|cells| Self::Cells(Cells::Owned(cells))),
                    }
                }
                Expr::Literal(value) => scalar(value).ok().map(Self::Constant),
                Expr::Parameter { index, .. } => scalar(parameters.known(*index)?).ok().map(Self::Constant),
                Expr::Binary { op: BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply, .. } => {
                    let column = super::projection::column(expr, schema, batch, parameters).ok()?.or_else(|| crate::vector::column(expr, schema, batch, parameters))?;
                    let cells = Cells::of(&column)?;
                    Some(Self::Cells(Cells::Owned((0..batch.len).map(|row| match cells.get(row).ok()? { None => Some(None), Some(cell) => owned(cell).map(Some) }).collect::<Option<_>>()?)))
                }
                _ => None,
            }
        }
        fn get(&self, row: usize) -> Result<Option<Cell<'a>>, ()> { match self { Self::Cells(cells) => cells.get(row), Self::Constant(cell) => Ok(*cell) } }
    }

    pub(super) fn truth(expr: &Expr, schema: &Schema, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>) -> Option<Truth> {
        use std::cmp::Ordering::*;
        let rows = batch.len;
        match expr {
            Expr::Binary { op: BinaryOp::And, left, right } => {
                let (left, right) = (truth(left, schema, batch, parameters)?, truth(right, schema, batch, parameters)?);
                Some(left.into_iter().zip(right).map(|pair| match pair { (Some(false), _) | (_, Some(false)) => Some(false), (Some(true), Some(true)) => Some(true), _ => None }).collect())
            }
            Expr::Binary { op: BinaryOp::Or, left, right } => {
                let (left, right) = (truth(left, schema, batch, parameters)?, truth(right, schema, batch, parameters)?);
                Some(left.into_iter().zip(right).map(|pair| match pair { (Some(true), _) | (_, Some(true)) => Some(true), (Some(false), Some(false)) => Some(false), _ => None }).collect())
            }
            Expr::Unary { op: UnaryOp::Not, expr } => Some(truth(expr, schema, batch, parameters)?.into_iter().map(|value| value.map(|value| !value)).collect()),
            Expr::Unary { op: op @ (UnaryOp::IsNull | UnaryOp::IsNotNull), expr } => {
                if let Expr::Column { id, .. } = expr.as_ref()
                    && let Column::Selected(column) = &batch.columns[schema.position(*id)?] {
                    return column.null_test(*op == UnaryOp::IsNull);
                }
                let operand = Operand::of(expr, schema, batch, parameters)?;
                (0..rows).map(|row| operand.get(row).ok().map(|cell| Some(cell.is_none() == (*op == UnaryOp::IsNull)))).collect()
            }
            Expr::Column { .. } => {
                let operand = Operand::of(expr, schema, batch, parameters)?;
                (0..rows).map(|row| match operand.get(row).ok()? { None => Some(None), Some(Cell::Bool(value)) => Some(Some(value)), Some(_) => None }).collect()
            }
            Expr::Binary { op, left, right } => {
                let (equality_only, accept): (bool, fn(std::cmp::Ordering) -> bool) = match op {
                    BinaryOp::Eq => (true, |ordering| ordering == Equal), BinaryOp::NotEq => (true, |ordering| ordering != Equal),
                    BinaryOp::Lt => (false, |ordering| ordering == Less), BinaryOp::LtEq => (false, |ordering| ordering != Greater),
                    BinaryOp::Gt => (false, |ordering| ordering == Greater), BinaryOp::GtEq => (false, |ordering| ordering != Less),
                    _ => return None,
                };
                let (left, right) = (Operand::of(left, schema, batch, parameters)?, Operand::of(right, schema, batch, parameters)?);
                (0..rows).map(|row| match (left.get(row).ok()?, right.get(row).ok()?) {
                    (Some(a), Some(b)) => order(a, b, equality_only).ok().map(|ordering| Some(accept(ordering))),
                    _ => Some(None),
                }).collect()
            }
            Expr::Call { name, args, .. } if name == "===" && args.len() == 2 => {
                let (left, right) = (Operand::of(&args[0], schema, batch, parameters)?, Operand::of(&args[1], schema, batch, parameters)?);
                (0..rows).map(|row| Some(Some(match (left.get(row).ok()?, right.get(row).ok()?) {
                    (None, None) => true,
                    (None, Some(_)) | (Some(_), None) => false,
                    (Some(Cell::Int(a)), Some(Cell::Int(b))) => a == b,
                    // Floats keep their category but compare by SQL equality, so NaN differs from itself.
                    (Some(Cell::Float(a)), Some(Cell::Float(b))) => a == b,
                    (Some(Cell::Int(_)), Some(Cell::Float(_))) | (Some(Cell::Float(_)), Some(Cell::Int(_))) => false,
                    (Some(Cell::Text(a)), Some(Cell::Text(b))) => a == b,
                    (Some(Cell::Bool(a)), Some(Cell::Bool(b))) => a == b,
                    (Some(Cell::Uuid(a)), Some(Cell::Uuid(b))) => a == b,
                    // Timestamps are equal only under the same zone label.
                    (Some(Cell::Micros(a, zone)), Some(Cell::Micros(b, other))) => zone == other && a == b,
                    _ => return None,
                }))).collect()
            }
            Expr::Between { operand, low, high, symmetric: false } => {
                let (operand, low, high) = (Operand::of(operand, schema, batch, parameters)?, Operand::of(low, schema, batch, parameters)?, Operand::of(high, schema, batch, parameters)?);
                (0..rows).map(|row| {
                    let (value, low, high) = (operand.get(row).ok()?, low.get(row).ok()?, high.get(row).ok()?);
                    let above = match (value, low) { (Some(value), Some(low)) => Some(order(value, low, false).ok()? != Less), _ => None };
                    let below = match (value, high) { (Some(value), Some(high)) => Some(order(value, high, false).ok()? != Greater), _ => None };
                    Some(match (above, below) { (Some(false), _) | (_, Some(false)) => Some(false), (Some(true), Some(true)) => Some(true), _ => None })
                }).collect()
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use rtdb_plan::{BinaryOp, ColumnId, Expr, Schema, UnaryOp};
    use rtdb_types::ScalarType;

    struct Random(u64);
    impl Random {
        fn next(&mut self) -> u64 { self.0 ^= self.0 << 13; self.0 ^= self.0 >> 7; self.0 ^= self.0 << 17; self.0 }
        fn below(&mut self, bound: u64) -> u64 { self.next() % bound }
    }

    fn field(id: u32, scalar: ScalarType) -> rtdb_plan::Field {
        rtdb_plan::Field { id: ColumnId(id), name: format!("c{id}"), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(scalar).maybe() }
    }
    fn arrow(array: ArrayRef, scalar: ScalarType) -> Column {
        Column::Arrow { field: Arc::new(ArrowField::new("c", array.data_type().clone(), true)), array, logical: Some(LogicalType::Scalar(scalar)) }
    }

    #[tokio::test]
    async fn scalar_projection_only_admits_its_referenced_input_cells() {
        struct Input(Option<ColumnBatch>);
        #[async_trait::async_trait]
        impl BatchCursor for Input {
            async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> { Ok(self.0.take()) }
        }

        let source = MemoryBudget::new(1 << 23);
        let payload = "x".repeat(1 << 21);
        let schema = Schema::new(vec![field(0, ScalarType::I64), field(1, ScalarType::Utf8)]).unwrap();
        let batch = ColumnBatch::settled(vec![
            arrow(Arc::new(Int64Array::from(vec![10, 20])), ScalarType::I64),
            arrow(Arc::new(StringArray::from(vec![payload.as_str(), "small"])), ScalarType::Utf8),
        ], 2, source.reserve(0).unwrap()).unwrap();
        let memory = MemoryBudget::new(1 << 18);
        let database = crate::DatabaseSnapshot {
            partitions: vec![],
            storage: Arc::new(rtdb_storage::cache::BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
        };
        let catalog = crate::CatalogSnapshot::new("xtdb", [("xtdb".into(), database)].into(), false).unwrap();
        let context = crate::QueryContext::new(catalog, Arc::clone(&memory), Timestamp(0), "UTC", Default::default(), &tokio_util::sync::CancellationToken::new()).unwrap();
        let execution = crate::context::Execution { context, parameters: Arc::new(crate::parameters::BoundParameters::infer(Vec::new(), &memory).unwrap()),
            outer: crate::context::OuterRows::empty(&memory).unwrap(), bindings: None, analysis: None };
        let modulo = Expr::binary(BinaryOp::Modulo, schema.fields()[0].expression(), Expr::Literal(Value::I64(3)));
        let mut cursor = BatchExpressions::new(Box::new(Input(Some(batch))), schema, vec![modulo], execution);
        let page = cursor.next_batch().await.unwrap().unwrap();
        let [Column::Arrow { array, .. }] = page.columns.as_slice() else { panic!("Expected typed Arrow modulo results"); };
        assert_eq!(array.as_any().downcast_ref::<Int64Array>().unwrap().values().as_ref(), &[1, 2]);
    }

    #[tokio::test]
    async fn residual_predicates_borrow_selected_struct_fields_and_list_cells() {
        let source = MemoryBudget::new(1 << 25);
        let large = "x".repeat(1 << 21);
        let ty = LogicalType::Struct(rtdb_types::StructFields::from([
            ("selected".into(), LogicalType::Scalar(ScalarType::I64)),
            ("untouched".into(), LogicalType::Scalar(ScalarType::Utf8)),
        ]));
        let field = Arc::new(rtdb_columnar::to_field("doc", &ty).unwrap());
        let content = rtdb_columnar::build_struct_array(
            match field.data_type() {
                DataType::Struct(fields) => fields.clone(),
                _ => panic!("record layout"),
            },
            &[
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![large.as_str(), large.as_str()])),
            ], 2, None,
        ).unwrap();
        let batch = ColumnBatch::settled(vec![Column::Arrow { field, array: content, logical: Some(ty.clone()) }],
            2, source.reserve(0).unwrap()).unwrap();
        let schema = Schema::new(vec![rtdb_plan::Field {
            id: ColumnId(0), name: "doc".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: ty,
        }]).unwrap();
        let memory = MemoryBudget::new(1 << 18);
        let database = crate::DatabaseSnapshot {
            partitions: vec![],
            storage: Arc::new(rtdb_storage::cache::BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
        };
        let catalog = crate::CatalogSnapshot::new("xtdb", [("xtdb".into(), database)].into(), false).unwrap();
        let context = crate::QueryContext::new(catalog, Arc::clone(&memory), Timestamp(0), "UTC", Default::default(),
            &tokio_util::sync::CancellationToken::new()).unwrap();
        let execution = crate::context::Execution {
            context, parameters: Arc::new(crate::parameters::BoundParameters::infer(Vec::new(), &memory).unwrap()),
            outer: crate::context::OuterRows::empty(&memory).unwrap(), bindings: None, analysis: None,
        };
        let leaf = Expr::Field {
            expr: Box::new(schema.fields()[0].expression()), name: "selected".into(), data_type: LogicalType::Scalar(ScalarType::I64),
        };
        let predicate = Expr::binary(BinaryOp::Eq, leaf, Expr::Literal(Value::I64(2)));
        assert_eq!(predicate_mask(&batch, &predicate, &schema, &execution).await.unwrap(), [false, true]);

        let ty = LogicalType::list(LogicalType::Scalar(ScalarType::Utf8));
        let array = rtdb_columnar::copy_array(&ty, &[Value::list(vec![Value::Utf8("keep".into()), Value::Utf8(large)])]).unwrap();
        let field = Arc::new(rtdb_columnar::to_field("xs", &ty).unwrap());
        let schema = Schema::new(vec![rtdb_plan::Field {
            id: ColumnId(0), name: "xs".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: ty.clone(),
        }]).unwrap();
        let batch = ColumnBatch::settled(vec![Column::Arrow { array, field, logical: Some(ty) }], 1, source.reserve(0).unwrap()).unwrap();
        let first = Expr::Subscript {
            expr: Box::new(schema.fields()[0].expression()), index: Box::new(Expr::Literal(Value::I64(0))),
            data_type: LogicalType::Scalar(ScalarType::Utf8).maybe(),
        };
        let predicate = Expr::binary(BinaryOp::Eq, first, Expr::Literal(Value::Utf8("keep".into())));
        assert_eq!(predicate_mask(&batch, &predicate, &schema, &execution).await.unwrap(), [true]);
    }

    #[test]
    fn row_copy_builds_an_arrow_record_from_compatible_nested_vectors() {
        let memory = MemoryBudget::new(1 << 20);
        let logical = LogicalType::List { kind: ListKind::List, element: Box::new(LogicalType::Scalar(ScalarType::Binary)) };
        let array = rtdb_columnar::copy_array(&logical, &[Value::list(vec![Value::Binary(vec![7; 1 << 16])])]).unwrap();
        let mut output = field(0, ScalarType::I64);
        output.data_type = logical.clone();
        let schema = Schema::new(vec![output.clone()]).unwrap();
        let column = Column::Arrow {
            array: Arc::clone(&array), field: Arc::new(rtdb_columnar::to_field(&output.name, &logical).unwrap()), logical: Some(logical),
        };
        let batch = ColumnBatch::settled(vec![column], 1, memory.reserve(0).unwrap()).unwrap();
        let expression = Expr::RowCopy(vec![output]);
        let bound = projection::bound(&expression, &schema, &batch).unwrap().unwrap();
        assert!(bound < 1 << 17);
        let result = projection::column(&expression, &schema, &batch, rtdb_expr::Parameters::Values(&[])).unwrap().unwrap();
        let Column::Arrow { array: record, .. } = result else { panic!("Expected columnar record"); };
        let record = record.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(Arc::ptr_eq(record.column(0), &array));
    }

    #[test]
    fn collected_lists_preserve_nested_record_presence_for_the_next_expression() {
        use arrow_array::RecordBatch;
        use rtdb_types::StructFields;

        let memory = MemoryBudget::new(1 << 20);
        let first = Value::Struct(StructFields::from([("a".into(), Value::I64(1))]));
        let second = Value::Struct(StructFields::from([("a".into(), Value::I64(2)), ("b".into(), Value::Utf8("present".into()))]));
        let read = |value: &Value| {
            let ty = value.logical_type();
            let field = rtdb_columnar::to_field("doc", &ty).unwrap();
            let array = rtdb_columnar::copy_array(&ty, std::slice::from_ref(value)).unwrap();
            let input = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(vec![field])), vec![array]).unwrap();
            ColumnBatch::from_owned(rtdb_columnar::OwnedBatch::new(input, &memory).unwrap()).unwrap()
        };
        let sources = [read(&first), read(&second), read(&Value::Null)];
        let mut output = field(0, ScalarType::I64);
        output.data_type = LogicalType::list(first.logical_type().join(&second.logical_type()));
        let homogeneous = ColumnBatch::take_rows(&sources, &[(0, 0)], &[0], &memory).unwrap();
        let (column, _charge) = homogeneous.collect_lists(vec![0, 1], vec![true], &output, &memory).unwrap();
        assert!(matches!(column, Column::Arrow { .. }));
        assert_eq!(column.value(0).unwrap(), Value::list(vec![first.clone()]));

        let heterogeneous = ColumnBatch::take_rows(&sources, &[(0, 0), (1, 0)], &[0], &memory).unwrap();
        let (column, _charge) = heterogeneous.collect_lists(vec![0, 2], vec![true], &output, &memory).unwrap();
        assert_eq!(column.value(0).unwrap(), Value::list(vec![first.clone(), second]));
        let sparse = ColumnBatch::take_rows(&sources, &[(0, 0), (2, 0)], &[0], &memory).unwrap();
        let (column, _charge) = sparse.collect_lists(vec![0, 2], vec![true], &output, &memory).unwrap();
        assert_eq!(column.value(0).unwrap(), Value::list(vec![first, Value::Null]));
    }

    #[test]
    fn nested_field_projection_masks_the_parent_without_rewriting_child_values() {
        let memory = MemoryBudget::new(1 << 20);
        let values = Arc::new(Int64Array::from(vec![7, 8]));
        let record = StructArray::new(
            vec![ArrowField::new("child", DataType::Int64, false)].into(),
            vec![values.clone()],
            Some(arrow_buffer::NullBuffer::from(vec![true, false])),
        );
        let source = Arc::new(ArrowField::new("record", record.data_type().clone(), true));
        let logical = rtdb_columnar::from_field(&source).unwrap();
        let mut input = field(0, ScalarType::I64);
        input.data_type = logical.clone();
        let schema = Schema::new(vec![input.clone()]).unwrap();
        let batch = ColumnBatch::settled(
            vec![Column::Arrow { array: Arc::new(record), field: source, logical: Some(logical) }],
            2, memory.reserve(0).unwrap(),
        ).unwrap();
        let expression = Expr::Field {
            expr: Box::new(input.expression()), name: "child".into(),
            data_type: LogicalType::Scalar(ScalarType::I64).maybe(),
        };
        let result = projection::column(&expression, &schema, &batch, rtdb_expr::Parameters::Values(&[])).unwrap().unwrap();
        assert_eq!(result.value(0).unwrap(), Value::I64(7));
        assert_eq!(result.value(1).unwrap(), Value::Null);
        let Column::Arrow { array, .. } = result else { panic!("Expected an Arrow field"); };
        let child = array.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(child.values().as_ptr(), values.values().as_ptr());

        let expression = Expr::Struct(vec![("record".into(), input.expression())]);
        let result = projection::column(&expression, &schema, &batch, rtdb_expr::Parameters::Values(&[])).unwrap().unwrap();
        let Column::Arrow { array, .. } = result else { panic!("Expected an Arrow struct"); };
        assert_eq!(array.as_any().downcast_ref::<StructArray>().unwrap().column(0).null_count(), 1);
    }

    #[test]
    fn ordered_gather_interleaves_arrow_batches_and_preserves_missing_cells() {
        let memory = MemoryBudget::new(1 << 20);
        let page = |columns| ColumnBatch::settled(columns, 2, memory.reserve(0).unwrap()).unwrap();
        let sources = [
            page(vec![
                arrow(Arc::new(Int64Array::from(vec![10, 20])), ScalarType::I64),
                Column::Arrow {
                    array: Arc::new(arrow_array::NullArray::new(2)),
                    field: Arc::new(ArrowField::new("c", DataType::Null, true)),
                    logical: Some(LogicalType::Null),
                },
            ]),
            page(vec![
                arrow(Arc::new(Int64Array::from(vec![30, 40])), ScalarType::I64),
                arrow(Arc::new(StringArray::from(vec!["first", "second"])), ScalarType::Utf8),
            ]),
        ];
        let result = ColumnBatch::take_rows(&sources, &[(1, 1), (0, 0), (1, 0)], &[0, 1], &memory).unwrap();
        assert!(result.columns.iter().all(|column| matches!(column, Column::Arrow { .. })));
        assert_eq!(result.columns[0].value(0).unwrap(), Value::I64(40));
        assert_eq!(result.columns[0].value(1).unwrap(), Value::I64(10));
        assert_eq!(result.columns[1].value(0).unwrap(), Value::Utf8("second".into()));
        assert_eq!(result.columns[1].value(1).unwrap(), Value::Null);
        assert_eq!(result.columns[1].value(2).unwrap(), Value::Utf8("first".into()));
    }

    fn batch(random: &mut Random, rows: usize, memory: &Arc<MemoryBudget>) -> ColumnBatch {
        let null = |random: &mut Random| random.below(5) == 0;
        let ints: Int64Array = (0..rows).map(|_| (!null(random)).then(|| random.below(21) as i64 - 10)).collect();
        let floats: Float64Array = (0..rows).map(|_| (!null(random)).then(|| float_cell(random))).collect();
        let texts: StringArray = (0..rows).map(|_| (!null(random)).then(|| ["a", "b", "ab", "B", ""][random.below(5) as usize])).collect();
        let bools: BooleanArray = (0..rows).map(|_| (!null(random)).then(|| random.below(2) == 0)).collect();
        let values = (0..rows).map(|_| if null(random) { Value::Null } else { Value::I64(random.below(21) as i64 - 10) }).collect();
        ColumnBatch::settled(vec![arrow(Arc::new(ints), ScalarType::I64), arrow(Arc::new(floats), ScalarType::F64), arrow(Arc::new(texts), ScalarType::Utf8),
            arrow(Arc::new(bools), ScalarType::Bool), Column::Values(values)], rows, memory.reserve(0).unwrap()).unwrap()
    }

    fn operand(random: &mut Random, schema: &Schema) -> Expr {
        let numeric = [0usize, 1, 4][random.below(3) as usize];
        match random.below(3) {
            0 => schema.fields()[numeric].expression(),
            1 => Expr::Literal([Value::I64(random.below(21) as i64 - 10), Value::I16(random.below(21) as i16 - 10), Value::I32(random.below(21) as i32 - 10)][random.below(3) as usize].clone()),
            _ => Expr::Literal(Value::F64(float_cell(random))),
        }
    }
    fn predicate(random: &mut Random, schema: &Schema, depth: u32) -> Expr {
        let comparison = [BinaryOp::Eq, BinaryOp::NotEq, BinaryOp::Lt, BinaryOp::LtEq, BinaryOp::Gt, BinaryOp::GtEq];
        match if depth == 0 { random.below(5) } else { random.below(8) } {
            0 => Expr::binary(comparison[random.below(6) as usize], schema.fields()[[0, 1, 4][random.below(3) as usize]].expression(), operand(random, schema)),
            1 => Expr::binary([BinaryOp::Eq, BinaryOp::NotEq][random.below(2) as usize], schema.fields()[2].expression(), Expr::Literal(Value::Utf8(["a", "b", "ab", "zz"][random.below(4) as usize].into()))),
            2 => Expr::Between { operand: Box::new(schema.fields()[[0, 1, 4][random.below(3) as usize]].expression()), low: Box::new(operand(random, schema)), high: Box::new(operand(random, schema)), symmetric: false },
            3 => Expr::Unary { op: [UnaryOp::IsNull, UnaryOp::IsNotNull][random.below(2) as usize], expr: Box::new(schema.fields()[random.below(5) as usize].expression()) },
            4 => schema.fields()[3].expression(),
            5 => Expr::binary(BinaryOp::And, predicate(random, schema, depth - 1), predicate(random, schema, depth - 1)),
            6 => Expr::binary(BinaryOp::Or, predicate(random, schema, depth - 1), predicate(random, schema, depth - 1)),
            7 if random.below(2) == 0 => {
                let arithmetic = Expr::binary(BinaryOp::Multiply, schema.fields()[[0usize, 1, 4][random.below(3) as usize]].expression(), operand(random, schema));
                if random.below(2) == 0 { Expr::Call { name: "===".into(), args: vec![arithmetic, operand(random, schema)], return_type: LogicalType::Scalar(ScalarType::Bool) } }
                else { Expr::binary(comparison[random.below(6) as usize], arithmetic, operand(random, schema)) }
            }
            _ => Expr::Unary { op: UnaryOp::Not, expr: Box::new(predicate(random, schema, depth - 1)) },
        }
    }

    fn arithmetic(random: &mut Random, schema: &Schema, depth: u32) -> Expr {
        let ops = [BinaryOp::Add, BinaryOp::Subtract, BinaryOp::Multiply];
        match if depth == 0 { random.below(3) } else { random.below(5) } {
            0 => schema.fields()[[0usize, 1][random.below(2) as usize]].expression(),
            1 => Expr::Literal([Value::I64(random.below(21) as i64 - 10), Value::I32(random.below(9) as i32 - 4), Value::F64((random.below(41) as f64 - 20.0) / 4.0), Value::I64(i64::MAX - random.below(3) as i64)][random.below(4) as usize].clone()),
            2 => schema.fields()[[0usize, 1, 2][random.below(3) as usize]].expression(),
            _ => Expr::binary(ops[random.below(3) as usize], arithmetic(random, schema, depth - 1), arithmetic(random, schema, depth - 1)),
        }
    }

    #[test]
    fn the_projection_kernel_agrees_with_the_evaluator_on_every_batch_it_computes() {
        let memory = MemoryBudget::new(1 << 28);
        let schema = Schema::new(vec![field(0, ScalarType::I64), field(1, ScalarType::F64), field(2, ScalarType::Utf8), field(3, ScalarType::Bool), field(4, ScalarType::I64)]).unwrap();
        let mut random = Random(0x2545f4914f6cdd1d);
        let mut computed = 0;
        for _ in 0..400 {
            let batch = batch(&mut random, 64, &memory);
            let expression = if random.below(3) == 0 {
                Expr::Struct(vec![("a".into(), arithmetic(&mut random, &schema, 2)), ("t".into(), schema.fields()[2].expression()), ("b".into(), arithmetic(&mut random, &schema, 2))])
            } else { arithmetic(&mut random, &schema, 3) };
            let Some(column) = projection::column(&expression, &schema, &batch, rtdb_expr::Parameters::Values(&[])).unwrap() else { continue };
            computed += 1;
            let scope = rtdb_expr::EvaluationScope::new(&memory).unwrap();
            let context = rtdb_expr::EvalContext { parameters: rtdb_expr::Parameters::Values(&[]), parameter_types: None, current_time: rtdb_types::Instant::from(Timestamp(0)), timezone: "UTC",
                outer_rows: &[], snapshot_token: None, allocation: Some(&scope), catalog: None };
            for row in 0..batch.len {
                let values: Vec<Value> = batch.columns.iter().map(|column| column.value(row).unwrap()).collect();
                let expected = rtdb_expr::evaluate_projection(&expression, &schema, &values, &context).unwrap();
                assert_eq!(column.value(row).unwrap(), expected, "{expression:?} on {values:?}");
            }
        }
        assert!(computed > 150, "the kernel should compute most generated projections, computed {computed}");
    }

    fn float_cell(random: &mut Random) -> f64 {
        let value = (random.below(41) as f64 - 20.0) / 2.0;
        if value == 0.0 && random.below(2) == 0 { -0.0 } else { value }
    }

    #[test]
    fn the_column_kernel_orders_negative_zero_below_zero_but_equal_to_it() {
        use rtdb_plan::BinaryOp::{Eq, GtEq, Lt};
        let memory = MemoryBudget::new(1 << 24);
        let schema = Schema::new(vec![field(1, ScalarType::F64)]).unwrap();
        let batch = ColumnBatch::settled(vec![arrow(Arc::new(Float64Array::from(vec![-0.0, 0.0])), ScalarType::F64)], 2, memory.reserve(0).unwrap()).unwrap();
        let column = Expr::Column { id: rtdb_plan::ColumnId(1), data_type: LogicalType::Scalar(ScalarType::F64) };
        for (op, literal, expected) in [(Lt, Value::F64(0.0), [true, false]), (Lt, Value::I64(0), [true, false]), (GtEq, Value::F64(0.0), [false, true]), (Eq, Value::F64(0.0), [true, true])] {
            let predicate = Expr::binary(op, column.clone(), Expr::Literal(literal));
            assert_eq!(kernel::truth(&predicate, &schema, &batch, rtdb_expr::Parameters::Values(&[])), Some(expected.map(Some).to_vec()), "{predicate:?}");
        }
    }

    #[test]
    fn the_column_kernel_compares_uuid_cells_as_the_scalar_evaluator_does() {
        use arrow_array::FixedSizeBinaryArray;
        let memory = MemoryBudget::new(1 << 28);
        let schema = Schema::new(vec![field(0, ScalarType::Uuid), field(1, ScalarType::Uuid)]).unwrap();
        let mut random = Random(0x2545f4914f6cdd1d);
        // Few distinct values, differing in their first, middle and last bytes, so equal and adjacent cells are common.
        let uuid = |random: &mut Random| { let mut bytes = [0u8; 16]; for at in [0, 7, 8, 15] { bytes[at] = [0, 1, 0x7f, 0x80, 0xff][random.below(5) as usize]; } bytes };
        let comparison = [BinaryOp::Eq, BinaryOp::NotEq, BinaryOp::Lt, BinaryOp::LtEq, BinaryOp::Gt, BinaryOp::GtEq];
        for round in 0..200 {
            let cells: Vec<Option<[u8; 16]>> = (0..64).map(|_| (random.below(5) != 0).then(|| uuid(&mut random))).collect();
            let stored = FixedSizeBinaryArray::try_from_sparse_iter_with_size(cells.iter().map(|cell| cell.as_ref().map(|bytes| bytes.as_slice())), 16).unwrap();
            let values = (0..64).map(|_| if random.below(5) == 0 { Value::Null } else { Value::Uuid(uuid(&mut random)) }).collect();
            let batch = ColumnBatch::settled(vec![arrow(Arc::new(stored), ScalarType::Uuid), Column::Values(values)], 64, memory.reserve(0).unwrap()).unwrap();
            let parameters = [Value::Uuid(uuid(&mut random)), Value::Uuid(uuid(&mut random))];
            let column = |random: &mut Random| schema.fields()[random.below(2) as usize].expression();
            let constant = |random: &mut Random| match random.below(3) {
                0 => Expr::Literal(Value::Uuid(uuid(random))),
                1 => Expr::Parameter { index: random.below(2) as usize, data_type: LogicalType::Scalar(ScalarType::Uuid) },
                _ => Expr::Literal(Value::Null),
            };
            let predicate = match round % 5 {
                0 => Expr::binary(comparison[random.below(6) as usize], column(&mut random), constant(&mut random)),
                1 => Expr::binary(comparison[random.below(6) as usize], constant(&mut random), column(&mut random)),
                2 => Expr::binary(comparison[random.below(6) as usize], schema.fields()[0].expression(), schema.fields()[1].expression()),
                3 => Expr::binary(BinaryOp::And, Expr::binary(BinaryOp::GtEq, column(&mut random), constant(&mut random)), Expr::binary(BinaryOp::Lt, column(&mut random), constant(&mut random))),
                _ => Expr::Call { name: "===".into(), args: vec![column(&mut random), constant(&mut random)], return_type: LogicalType::Scalar(ScalarType::Bool) },
            };
            let truth = kernel::truth(&predicate, &schema, &batch, rtdb_expr::Parameters::Values(&parameters)).unwrap_or_else(|| panic!("the kernel evaluates {predicate:?}"));
            let scope = rtdb_expr::EvaluationScope::new(&memory).unwrap();
            let context = rtdb_expr::EvalContext { parameters: rtdb_expr::Parameters::Values(&parameters), parameter_types: None, current_time: rtdb_types::Instant::from(Timestamp(0)), timezone: "UTC",
                outer_rows: &[], snapshot_token: None, allocation: Some(&scope), catalog: None };
            for (row, expected) in truth.iter().enumerate() {
                let values: Vec<Value> = batch.columns.iter().map(|column| column.value(row).unwrap()).collect();
                let actual = match rtdb_expr::evaluate(&predicate, &schema, &values, &context).unwrap() { Value::Bool(value) => Some(value), Value::Null => None, other => panic!("{other:?}") };
                assert_eq!(actual, *expected, "{predicate:?} on {values:?}");
            }
        }
    }

    #[test]
    fn the_column_kernel_agrees_with_the_scalar_evaluator_on_every_row_it_evaluates() {
        let memory = MemoryBudget::new(1 << 28);
        let schema = Schema::new(vec![field(0, ScalarType::I64), field(1, ScalarType::F64), field(2, ScalarType::Utf8), field(3, ScalarType::Bool), field(4, ScalarType::I64)]).unwrap();
        let mut random = Random(0x9e3779b97f4a7c15);
        let mut evaluated = 0;
        for _ in 0..400 {
            let batch = batch(&mut random, 64, &memory);
            let predicate = predicate(&mut random, &schema, 3);
            let Some(truth) = kernel::truth(&predicate, &schema, &batch, rtdb_expr::Parameters::Values(&[])) else { continue };
            evaluated += 1;
            let scope = rtdb_expr::EvaluationScope::new(&memory).unwrap();
            let context = rtdb_expr::EvalContext { parameters: rtdb_expr::Parameters::Values(&[]), parameter_types: None, current_time: rtdb_types::Instant::from(Timestamp(0)), timezone: "UTC",
                outer_rows: &[], snapshot_token: None, allocation: Some(&scope), catalog: None };
            for (row, expected) in truth.iter().enumerate() {
                let values: Vec<Value> = batch.columns.iter().map(|column| column.value(row).unwrap()).collect();
                let actual = match rtdb_expr::evaluate(&predicate, &schema, &values, &context).unwrap() { Value::Bool(value) => Some(value), Value::Null => None, other => panic!("{other:?}") };
                assert_eq!(actual, *expected, "{predicate:?} on {values:?}");
            }
        }
        assert!(evaluated > 200, "the kernel should take most generated predicates, took {evaluated}");
    }

    mod mutations {
        //! The shapes an UPDATE lowers to: its strict-equality filter, and the canonical put's `COALESCE`d, cast valid
        //! times and row-copied document, over cells that include nulls, NaN, signed zeros and zoned timestamps.
        use super::*;
        use arrow_array::TimestampMicrosecondArray;
        use rtdb_plan::CastTarget;
        use rtdb_types::TimeUnit;

        fn utc() -> ScalarType { ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) } }
        fn local() -> ScalarType { ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: None } }
        fn mixed() -> LogicalType {
            LogicalType::from_legs([LogicalType::Scalar(ScalarType::I64), LogicalType::Scalar(ScalarType::F64), LogicalType::Scalar(ScalarType::Utf8), LogicalType::Scalar(utc()), LogicalType::Null])
        }
        /// Each column's declared type, and whether its cells read through a nullable logical type.
        fn types() -> Vec<(LogicalType, bool)> {
            vec![(LogicalType::Scalar(ScalarType::I64), false), (LogicalType::Scalar(ScalarType::F64), true), (LogicalType::Scalar(ScalarType::Utf8), false),
                (LogicalType::Scalar(ScalarType::Bool), true), (LogicalType::Scalar(utc()), false), (LogicalType::Scalar(utc()), true),
                (LogicalType::Scalar(local()), false), (mixed(), false), (LogicalType::Scalar(ScalarType::F64), false), (LogicalType::Scalar(ScalarType::DateDay), true)]
        }
        fn schema() -> Schema {
            Schema::new(types().into_iter().enumerate().map(|(id, (logical, _))| rtdb_plan::Field { id: ColumnId(id as u32), name: format!("c{id}"), qualifier: None, qualified_only: false, unnamed: false, data_type: logical.maybe() }).collect()).unwrap()
        }
        fn float(random: &mut Random) -> f64 { [f64::NAN, -0.0, 0.0, 1.5, -2.0, 1.01, f64::INFINITY][random.below(7) as usize] }
        fn micros(random: &mut Random) -> i64 { [0, 1, -1, 1_000_000, i64::MAX, i64::MIN][random.below(6) as usize] }
        fn zoned(value: i64, zone: Option<&str>) -> Value { Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: zone.map(Into::into) } }

        fn batch(random: &mut Random, rows: usize, memory: &Arc<MemoryBudget>) -> ColumnBatch {
            let null = |random: &mut Random| random.below(4) == 0;
            let arrow = |array: ArrayRef, (logical, maybe): (LogicalType, bool)| Column::Arrow { field: Arc::new(ArrowField::new("c", array.data_type().clone(), true)), array, logical: Some(if maybe { logical.maybe() } else { logical }) };
            let types = types();
            let ints: Int64Array = (0..rows).map(|_| (!null(random)).then(|| random.below(5) as i64 - 2)).collect();
            let floats: Float64Array = (0..rows).map(|_| (!null(random)).then(|| float(random))).collect();
            let texts: StringArray = (0..rows).map(|_| (!null(random)).then(|| ["a", "b", ""][random.below(3) as usize])).collect();
            let bools: BooleanArray = (0..rows).map(|_| (!null(random)).then(|| random.below(2) == 0)).collect();
            let times = |random: &mut Random, nulls: bool| -> Vec<Option<i64>> { (0..rows).map(|_| (!(nulls && null(random))).then(|| micros(random))).collect() };
            let instants = TimestampMicrosecondArray::from(times(random, false)).with_timezone("UTC");
            let bounds = TimestampMicrosecondArray::from(times(random, true)).with_timezone("UTC");
            let locals = TimestampMicrosecondArray::from(times(random, true));
            let values = (0..rows).map(|_| match random.below(6) {
                0 => Value::Null, 1 => Value::I64(random.below(5) as i64 - 2), 2 => Value::F64(float(random)), 3 => Value::Utf8("a".into()),
                4 => zoned(micros(random), Some("UTC")), _ => Value::DateDay(random.below(3) as i32),
            }).collect();
            let plain: Float64Array = (0..rows).map(|_| (!null(random)).then(|| float(random))).collect();
            let dates: arrow_array::Date32Array = (0..rows).map(|_| (!null(random)).then(|| random.below(5) as i32 - 2)).collect();
            let columns = vec![arrow(Arc::new(ints), types[0].clone()), arrow(Arc::new(floats), types[1].clone()), arrow(Arc::new(texts), types[2].clone()),
                arrow(Arc::new(bools), types[3].clone()), arrow(Arc::new(instants), types[4].clone()), arrow(Arc::new(bounds), types[5].clone()),
                arrow(Arc::new(locals), types[6].clone()), Column::Values(values), arrow(Arc::new(plain), types[8].clone()), arrow(Arc::new(dates), types[9].clone())];
            ColumnBatch::settled(columns, rows, memory.reserve(0).unwrap()).unwrap()
        }

        fn column(schema: &Schema, id: usize) -> Expr { schema.fields()[id].expression() }
        fn literal(random: &mut Random) -> Expr {
            Expr::Literal(match random.below(8) {
                0 => Value::I64(random.below(5) as i64 - 2), 1 => Value::F64(float(random)), 2 => Value::Utf8("a".into()), 3 => Value::Bool(true),
                4 => zoned(micros(random), Some("UTC")), 5 => zoned(micros(random), None), 6 => Value::I32(1), _ => Value::Null,
            })
        }
        fn operand(random: &mut Random, schema: &Schema) -> Expr {
            match random.below(5) {
                0 | 1 => column(schema, random.below(10) as usize),
                2 => literal(random),
                3 => Expr::binary(BinaryOp::Multiply, column(schema, [1, 8][random.below(2) as usize]), Expr::Literal(Value::F64(1.01))),
                _ => Expr::binary(BinaryOp::Add, column(schema, 0), Expr::Literal(Value::I64(1))),
            }
        }
        fn strict(left: Expr, right: Expr) -> Expr { Expr::Call { name: "===".into(), args: vec![left, right], return_type: LogicalType::Scalar(ScalarType::Bool) } }
        fn predicate(random: &mut Random, schema: &Schema) -> Expr {
            let comparison = strict(operand(random, schema), operand(random, schema));
            match random.below(3) {
                0 => comparison,
                1 => Expr::Unary { op: UnaryOp::Not, expr: Box::new(comparison) },
                _ => Expr::binary(BinaryOp::Or, Expr::Unary { op: UnaryOp::Not, expr: Box::new(comparison) }, Expr::Unary { op: UnaryOp::Not, expr: Box::new(strict(operand(random, schema), operand(random, schema))) }),
            }
        }
        fn coalesce(random: &mut Random, schema: &Schema) -> Expr {
            let times = [4usize, 5, 5, 6];
            let args = (0..1 + random.below(3)).map(|_| match random.below(4) {
                0 | 1 => column(schema, if random.below(3) == 0 { random.below(10) as usize } else { times[random.below(4) as usize] }),
                2 => Expr::Literal(if random.below(2) == 0 { Value::Null } else { zoned(micros(random), Some("UTC")) }),
                _ => literal(random),
            }).collect();
            if random.below(4) == 0 { Expr::Call { name: "coalesce".into(), args, return_type: LogicalType::Scalar(utc()).maybe() } } else { Expr::Coalesce(args) }
        }
        fn projection(random: &mut Random, schema: &Schema) -> Expr {
            let cast = |inner: Expr, random: &mut Random| {
                let target = [LogicalType::Scalar(utc()), LogicalType::Scalar(utc()).maybe(), LogicalType::Scalar(local()), LogicalType::Scalar(ScalarType::F64), LogicalType::Scalar(ScalarType::I64), LogicalType::Scalar(ScalarType::Utf8)][random.below(6) as usize].clone();
                Expr::Cast { expr: Box::new(inner), target: CastTarget::Fixed(target) }
            };
            match random.below(5) {
                0 => coalesce(random, schema),
                1 => { let inner = coalesce(random, schema); cast(inner, random) }
                2 => { let inner = operand(random, schema); cast(inner, random) }
                3 => Expr::Literal(zoned(micros(random), Some("UTC"))),
                _ => Expr::RowCopy(schema.fields().iter().filter(|_| random.below(2) == 0).cloned().collect()),
            }
        }

        /// Equal cells, with floats compared by their bits.
        fn same(left: &Value, right: &Value) -> bool {
            match (left, right) {
                (Value::F64(a), Value::F64(b)) => a.to_bits() == b.to_bits(),
                (Value::Struct(a), Value::Struct(b)) => a.len() == b.len() && a.iter().zip(b.iter()).all(|((an, av), (bn, bv))| an == bn && same(av, bv)),
                (a, b) => a == b,
            }
        }

        #[test]
        fn column_kernels_agree_with_the_scalar_evaluator_on_every_batch_they_compute() {
            let memory = MemoryBudget::new(1 << 28);
            let schema = schema();
            let mut random = Random(0x853c_49e6_748f_ea9b);
            let (mut predicates, mut projections, mut conformed) = (0, 0, 0);
            for round in 0..3000 {
                let batch = batch(&mut random, 32, &memory);
                let scope = rtdb_expr::EvaluationScope::new(&memory).unwrap();
                let context = rtdb_expr::EvalContext { parameters: rtdb_expr::Parameters::Values(&[]), parameter_types: None, current_time: rtdb_types::Instant::from(Timestamp(0)), timezone: "UTC",
                    outer_rows: &[], snapshot_token: None, allocation: Some(&scope), catalog: None };
                let rows: Vec<Vec<Value>> = (0..batch.len).map(|row| batch.columns.iter().map(|column| column.value(row).unwrap()).collect()).collect();
                if round % 2 == 0 {
                    let predicate = predicate(&mut random, &schema);
                    let Some(truth) = kernel::truth(&predicate, &schema, &batch, rtdb_expr::Parameters::Values(&[])) else { continue };
                    predicates += 1;
                    for (values, expected) in rows.iter().zip(truth) {
                        let actual = match rtdb_expr::evaluate(&predicate, &schema, values, &context) {
                            Ok(Value::Bool(value)) => Some(value), Ok(Value::Null) => None,
                            other => panic!("the kernel computed {predicate:?} where the evaluator returns {other:?} on {values:?}"),
                        };
                        assert_eq!(actual, expected, "{predicate:?} on {values:?}");
                    }
                } else {
                    let expression = projection(&mut random, &schema);
                    assert!(projection::shaped(&expression), "{expression:?}");
                    if projection::bound(&expression, &schema, &batch).unwrap().is_none() { continue; }
                    let Some(column) = projection::column(&expression, &schema, &batch, rtdb_expr::Parameters::Values(&[])).unwrap() else { continue };
                    projections += 1;
                    let cells: Vec<Value> = (0..batch.len).map(|row| column.value(row).unwrap()).collect();
                    for (values, actual) in rows.iter().zip(&cells) {
                        let expected = rtdb_expr::evaluate_projection(&expression, &schema, values, &context)
                            .unwrap_or_else(|error| panic!("the kernel computed {expression:?} where the evaluator raises {error:?} on {values:?}"));
                        assert!(same(actual, &expected), "{expression:?} on {values:?}: evaluator {expected:?}, kernel {actual:?}");
                    }
                    let Column::Arrow { logical: Some(logical), .. } = &column else { continue };
                    // The result may order a record's fields differently from the copy that produced it.
                    let reordered = match logical { LogicalType::Struct(fields) => LogicalType::Struct(fields.iter().rev().map(|(name, ty)| (name.clone(), ty.clone().maybe())).collect()), other => other.clone() };
                    for target in [logical.clone(), logical.clone().maybe(), reordered] {
                        let field = rtdb_columnar::to_field("value", &target).unwrap();
                        let Some(kept) = conform(&column, &target, &field) else { continue };
                        conformed += 1;
                        let copied = rtdb_columnar::copy_array(&target, &cells).unwrap();
                        assert_eq!(kept.data_type(), copied.data_type(), "{expression:?} as {target:?}");
                        for (row, cell) in cells.iter().enumerate() {
                            let (kept, copied) = (rtdb_columnar::read_value(&field, kept.as_ref(), row).unwrap(), rtdb_columnar::read_value(&field, copied.as_ref(), row).unwrap());
                            assert!(same(&kept, &copied) && kept.is_null() == cell.is_null(), "{expression:?} as {target:?} at {row}: kept {kept:?}, copied {copied:?}");
                        }
                    }
                }
            }
            assert!(predicates > 450 && projections > 500 && conformed > 150, "the kernels should compute most generated expressions: {predicates} predicates, {projections} projections, {conformed} kept");
        }

        #[test]
        fn the_canonical_put_of_an_update_runs_entirely_in_the_kernels() {
            let memory = MemoryBudget::new(1 << 24);
            let schema = schema();
            let batch = batch(&mut Random(7), 16, &memory);
            let instant = |value: Expr| Expr::Cast { expr: Box::new(value), target: CastTarget::fixed(LogicalType::Scalar(utc())) };
            let now = Expr::Literal(zoned(42, Some("UTC")));
            let expressions = [instant(Expr::Coalesce(vec![column(&schema, 4), now])), instant(Expr::Coalesce(vec![column(&schema, 5), Expr::Literal(zoned(i64::MAX, Some("UTC")))])),
                Expr::RowCopy(vec![schema.fields()[0].clone(), schema.fields()[1].clone(), schema.fields()[2].clone()])];
            for expression in expressions {
                let column = projection::column(&expression, &schema, &batch, rtdb_expr::Parameters::Values(&[])).unwrap();
                assert!(matches!(column, Some(Column::Arrow { .. })), "{expression:?} should compute to an Arrow column");
            }
        }
    }
}

/// Reorders, selects or renames a batch's columns, for a projection whose expressions are all input columns.
pub(crate) struct BatchProject { input: BoxBatchCursor, positions: Vec<usize>, memory: Arc<MemoryBudget> }
impl BatchProject { pub fn new(input: BoxBatchCursor, positions: Vec<usize>, memory: Arc<MemoryBudget>) -> Self { Self { input, positions, memory } } }
#[async_trait::async_trait]
impl BatchCursor for BatchProject {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        tokio::task::consume_budget().await;
        let Some(batch) = poll_nested_batch(self.input.as_mut()).await? else { return Ok(None) };
        let len = batch.len;
        let bound = self.positions.iter().try_fold(256usize, |bound, position| bound.checked_add(batch.columns[*position].retained_bytes()?).ok_or_else(size_error))?;
        let reservation = self.memory.reserve(bound)?;
        let mut columns: Vec<Option<Column>> = batch.columns.into_iter().map(Some).collect();
        let mut uses = vec![0usize; columns.len()];
        for &position in &self.positions { uses[position] += 1; }
        let mut output = Vec::with_capacity(self.positions.len());
        for &position in &self.positions {
            uses[position] -= 1;
            output.push(if uses[position] == 0 { columns[position].take().ok_or_else(|| invalid("column-project", "Projected column was already moved"))? }
                else { columns[position].as_ref().map(Column::duplicate).ok_or_else(|| invalid("column-project", "Projected column is absent"))? });
        }
        ColumnBatch::settled(output, len, reservation).map(Some)
    }
    fn skip_complete_pages(&mut self, rows: u64) -> Result<u64> { self.input.skip_complete_pages(rows) }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { self.input.trace_attributes(span) }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
}
impl Column {
    fn duplicate(&self) -> Self {
        match self {
            Self::Arrow { array, field, logical } => Self::Arrow { array: Arc::clone(array), field: Arc::clone(field), logical: logical.clone() },
            Self::Selected(column) => Self::Selected(column.clone()),
            Self::Values(values) => Self::Values(values.clone()),
        }
    }
}

/// Projects batches through expressions computed over whole columns where the kernel's arithmetic is exactly the
/// evaluator's, and row by row through the evaluator for any expression or batch where it isn't.
pub(crate) struct BatchExpressions { input: BoxBatchCursor, schema: rtdb_plan::Schema, expressions: Vec<rtdb_plan::Expr>, clocked: Option<Vec<rtdb_plan::Expr>>, cosine: Option<Vec<Option<crate::cosine::Projection>>>, execution: crate::context::Execution, validate_input: bool }
impl BatchExpressions {
    pub fn new(input: BoxBatchCursor, schema: rtdb_plan::Schema, expressions: Vec<rtdb_plan::Expr>, execution: crate::context::Execution) -> Self { Self { input, schema, expressions, clocked: None, cosine: None, execution, validate_input: false } }
    pub fn validating_input(mut self, validate: bool) -> Self {
        self.validate_input = validate;
        self
    }
}

/// The expressions with each read of the query clock replaced by the instant the evaluator reads, for the kernels;
/// a read the evaluator fails stays a call, which leaves its expression to the evaluator.
async fn read_clock(expressions: &[rtdb_plan::Expr], execution: &crate::context::Execution) -> Result<Vec<rtdb_plan::Expr>> {
    let mut reads = Vec::new();
    for expression in expressions {
        expression.visit(&mut |expr| if let rtdb_plan::Expr::Call { name, args, .. } = expr && projection::clock(name, args) && !reads.contains(expr) { reads.push(expr.clone()); });
    }
    let schema = rtdb_plan::Schema::default();
    let mut instants = Vec::with_capacity(reads.len());
    for read in reads {
        let scope = execution.scope()?;
        if let Ok(value) = execution.evaluate(&scope, &read, &schema, &[]).await { instants.push((read, value)); }
    }
    Ok(expressions.iter().map(|expression| {
        let mut expression = expression.clone();
        expression.rewrite(&mut |expr| if let Some((_, value)) = instants.iter().find(|(read, _)| read == expr) { *expr = rtdb_plan::Expr::Literal(value.clone()); });
        expression
    }).collect())
}
#[async_trait::async_trait]
impl BatchCursor for BatchExpressions {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        tokio::task::consume_budget().await;
        let Some(batch) = poll_nested_batch(self.input.as_mut()).await? else { return Ok(None) };
        self.execution.context.check_cancelled()?;
        if self.validate_input { batch.validate_readable(&self.execution.context)?; }
        if self.clocked.is_none() { self.clocked = Some(read_clock(&self.expressions, &self.execution).await?); }
        let clocked = self.clocked.as_deref().unwrap_or_default();
        if self.cosine.is_none() && batch.len != 0 {
            self.cosine = Some(clocked.iter().map(|expr| {
                crate::cosine::Projection::bind(expr, &self.schema, &self.execution.parameters, &self.execution.context.memory)
            }).collect::<Result<_>>()?);
        }
        let parameters = self.execution.parameters.evaluator();
        let mut columns = Vec::with_capacity(self.expressions.len());
        let mut reservation = self.execution.context.memory.reserve(256)?;
        for (index, (expression, kernel)) in self.expressions.iter().zip(clocked).enumerate() {
            if let Some(projection) = self.cosine.as_ref().and_then(|items| items[index].as_ref()) {
                let bytes = batch.len.checked_mul(9).and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)?;
                if let Ok(scratch) = self.execution.context.memory.reserve(bytes)
                    && let Some(column) = projection.column(&batch) {
                    let lease = Arc::get_mut(&mut reservation).expect("Batch projection owns its reservation");
                    drop(scratch);
                    lease.grow_to(lease.bytes().checked_add(column.retained_bytes()?).ok_or_else(size_error)?)?;
                    columns.push(column);
                    continue;
                }
            }
            if let Some(column) = search::column(kernel, &self.schema, &batch, parameters, &self.execution.context)? {
                let lease = Arc::get_mut(&mut reservation).expect("Batch projection owns its reservation");
                lease.grow_to(lease.bytes().checked_add(column.retained_bytes()?).ok_or_else(size_error)?)?;
                columns.push(column);
                continue;
            }
            if let Some((column, mut charge)) = projection::coalesce_vectors(kernel, &self.schema, &batch, &self.execution.context.memory)? {
                let lease = Arc::get_mut(&mut reservation).expect("Batch projection owns its reservation");
                lease.absorb(Arc::get_mut(&mut charge).expect("A vector selection owns its reservation"))?;
                columns.push(column);
                continue;
            }
            // An ordinal such as 0::BIGINT is shared by every row of a probe.
            if batch.len != 0 && matches!(expression, rtdb_plan::Expr::Cast { expr, .. } if matches!(expr.as_ref(), rtdb_plan::Expr::Literal(_))) {
                let scope = self.execution.scope()?;
                let mut value = self.execution.evaluate(&scope, expression, &rtdb_plan::Schema::default(), &[]).await?;
                value.admit_expression_dates()?;
                rtdb_expr::validate_projection_value(&value)?;
                let bytes = repeated_cell_bound(&value, batch.len)?;
                let lease = Arc::get_mut(&mut reservation).expect("batch projection owns its reservation");
                lease.grow_to(lease.bytes().checked_add(bytes).ok_or_else(size_error)?)?;
                columns.push(repeat_cell(&value, batch.len)?);
                continue;
            }
            if let rtdb_plan::Expr::Call { name, args, .. } = kernel
                && name == "_patch"
                && let Some((column, _scratch)) = patch::merge(args, &self.schema, &batch, &self.execution.context.memory)? {
                let lease = Arc::get_mut(&mut reservation).expect("The patch projection owns its reservation");
                lease.grow_to(lease.bytes().checked_add(column.retained_bytes()?).ok_or_else(size_error)?)?;
                columns.push(column);
                continue;
            }
            if let Some(bound) = projection::bound(kernel, &self.schema, &batch)? {
                let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("column-size", "A column page's charge is shared"))?;
                let retained = lease.bytes();
                let next = lease.bytes().checked_add(bound).ok_or_else(size_error)?;
                match lease.grow_to(next) {
                    Ok(()) => {
                        if let Some(column) = projection::column(kernel, &self.schema, &batch, parameters)? {
                            if !matches!(expression, rtdb_plan::Expr::Column { .. } | rtdb_plan::Expr::RowCopy(_))
                                && let Column::Arrow { field, array, .. } = &column
                            {
                                rtdb_columnar::validate_projected_decimals(field, array.as_ref())?;
                            }
                            columns.push(column);
                            continue;
                        }
                        lease.shrink_to(retained)?;
                    }
                    Err(error) if matches!(kernel, rtdb_plan::Expr::List(_)) && error.code == "rtdb/memory-budget-exceeded" => {},
                    Err(error) => return Err(error),
                }
            }
            if let Some(column) = crate::vector::column(expression, &self.schema, &batch, parameters) {
                // A vectorised column's size is known once computed; it is charged before the next is built.
                let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("column-size", "A column page's charge is shared"))?;
                lease.grow_to(lease.bytes().checked_add(column.retained_bytes()?).ok_or_else(size_error)?)?;
                columns.push(column);
                continue;
            }
            let metadata = self.execution.scope()?;
            let invocation = self.execution.prepare_expression(expression, &self.schema, &metadata)?;
            let scope = self.execution.scope()?;
            let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("column-size", "A column page's charge is shared"))?;
            let retained = lease.bytes();
            lease.grow_to(retained.checked_add(batch.len.checked_mul(std::mem::size_of::<Value>()).ok_or_else(size_error)?).ok_or_else(size_error)?)?;
            let mut values = Vec::with_capacity(batch.len);
            for row in 0..batch.len {
                let input = BatchRow { batch: &batch, row, admit_dates: false };
                let value = self.execution.evaluate_invocation_row(
                    &scope, &invocation, rtdb_expr::ExpressionRow::Borrowed(&input),
                ).await?;
                // The value is charged before the evaluator's scope releases what it allocated it under.
                lease.grow_to(lease.bytes().checked_add(value.retained_bytes()?).ok_or_else(size_error)?)?;
                values.push(value);
                scope.reset()?;
            }
            if let Some((column, scratch)) = scalar_result(&values, &self.execution.context.memory)? {
                let bytes = retained.checked_add(column.retained_bytes()?).ok_or_else(size_error)?;
                let admission = if bytes <= lease.bytes() { Ok(()) } else { lease.grow_to(bytes) };
                match admission {
                    Ok(()) => {
                        drop(values);
                        lease.shrink_to(bytes)?;
                        drop(scratch);
                        columns.push(column);
                    }
                    Err(error) if error.code == "rtdb/memory-budget-exceeded" => columns.push(Column::Values(values)),
                    Err(error) => return Err(error),
                }
            } else {
                columns.push(Column::Values(values));
            }
        }
        let output = ColumnBatch::settled(columns, batch.len, reservation)?;
        if output.columns.iter().any(|column| matches!(column, Column::Values(values) if !values.is_empty())) {
            return output.pack_evaluated(&self.execution.context.memory).map(Some);
        }
        Ok(Some(output))
    }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { self.input.trace_attributes(span) }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
}

/// Packs scalar results whose Arrow cells read back as the same values and exact execution types.
pub(crate) fn scalar_result(values: &[Value], memory: &Arc<MemoryBudget>) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let Some(ty) = scalar_result_type(values.iter()) else { return Ok(None); };
    match pack_scalar_values(values, ty, memory) {
        Ok(column) => Ok(Some(column)),
        Err(error) if error.code == "rtdb/memory-budget-exceeded" => Ok(None),
        Err(error) => Err(error),
    }
}

fn repeated_cell_bound(value: &Value, count: usize) -> Result<usize> {
    let ty = value.logical_type();
    let metadata = ty.retained_bytes()?.checked_mul(8).ok_or_else(size_error)?;
    let builder = rtdb_columnar::estimate_builder_bytes(&ty, std::slice::from_ref(value))?;
    count.checked_mul(std::mem::size_of::<(usize, usize)>())
        .and_then(|bytes| bytes.checked_add(builder))
        .and_then(|bytes| bytes.checked_add(metadata))
        .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)
}

fn repeat_cell(value: &Value, count: usize) -> Result<Column> {
    let ty = value.logical_type();
    let (array, field) = (
        rtdb_columnar::copy_array(&ty, std::slice::from_ref(value))?,
        Arc::new(rtdb_columnar::to_field("value", &ty)?),
    );
    Ok(Column::Selected(SelectedColumn::new(
        vec![Some((array, field, Some(ty), None))],
        Arc::new(vec![(0, 0); count]),
    )))
}

fn pack_scalar_values(values: &[Value], ty: LogicalType, memory: &Arc<MemoryBudget>) -> Result<(Column, Arc<MemoryReservation>)> {
    let bound = rtdb_columnar::estimate_builder_bytes(&ty, values)?;
    let scratch = memory.reserve(bound)?;
    let array = rtdb_columnar::copy_array(&ty, values)?;
    let field = Arc::new(rtdb_columnar::to_field("value", &ty)?);
    Ok((Column::Arrow { array, field, logical: Some(ty) }, scratch))
}

pub(crate) fn pack_evaluated_column(values: &[Value], memory: &Arc<MemoryBudget>) -> Result<(Column, Arc<MemoryReservation>)> {
    if let Some(ty) = scalar_result_type(values.iter()) {
        return pack_scalar_values(values, ty, memory);
    }
    let bytes = values.len().checked_mul(
        2 * std::mem::size_of::<(usize, usize)>() + 2 * std::mem::size_of::<Option<StoredVector>>(),
    ).and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)?;
    let mut inventory = memory.reserve(bytes)?;
    let mut sources = Vec::new();
    let mut cells = Vec::with_capacity(values.len());
    let cell_type = |value: &Value| {
        if matches!(value, Value::Undefined) { LogicalType::Null } else { value.logical_type() }
    };
    if let Some(ty) = float_vector_type(values) {
        let field = Arc::new(rtdb_columnar::to_field("value", &ty)?);
        let mut custody = memory.reserve(rtdb_columnar::estimate_builder_bytes(&ty, values)?)?;
        let array = rtdb_columnar::copy_array(&ty, values)?;
        let lease = Arc::get_mut(&mut custody).expect("The evaluated column owns its array");
        let retained = array.get_array_memory_size().checked_add(field.size()).ok_or_else(size_error)?;
        if retained > lease.bytes() { lease.grow_to(retained)?; } else { lease.shrink_to(retained)?; }
        drop(inventory);
        return Ok((Column::Arrow { array, field, logical: Some(ty) }, custody));
    }
    let mut start = 0;
    while start < values.len() {
        let ty = cell_type(&values[start]);
        let field = Arc::new(rtdb_columnar::to_field("value", &ty)?);
        let mut end = start + 1;
        while end < values.len()
            && rtdb_columnar::to_field("value", &cell_type(&values[end]))? == *field {
            end += 1;
        }
        let run = &values[start..end];
        let bound = rtdb_columnar::estimate_builder_bytes(&ty, run)?;
        let mut custody = memory.reserve(bound)?;
        let array = rtdb_columnar::copy_array(&ty, run)?;
        let lease = Arc::get_mut(&mut custody).expect("The evaluated column owns its run");
        let retained = array.get_array_memory_size().checked_add(field.size()).ok_or_else(size_error)?;
        if retained > lease.bytes() { lease.grow_to(retained)?; } else { lease.shrink_to(retained)?; }
        if start == 0 && end == values.len() {
            return Ok((Column::Arrow { array, field, logical: Some(ty) }, custody));
        }
        let source = sources.len();
        cells.extend((0..run.len()).map(|row| (source, row)));
        sources.push(Some((array, field, Some(ty), Some(custody))));
        start = end;
    }
    let column = Column::Selected(SelectedColumn::new(sources, Arc::new(cells)));
    let bytes = column.retained_bytes()?.checked_add(256).ok_or_else(size_error)?;
    let lease = Arc::get_mut(&mut inventory).expect("The evaluated column owns its selection");
    if bytes > lease.bytes() { lease.grow_to(bytes)?; } else { lease.shrink_to(bytes)?; }
    Ok((column, inventory))
}

/// The joined type of a column of packed vectors of one float width and list kind, and nulls,
/// which pack into one Arrow column from their typed components rather than a run per nullability.
fn float_vector_type(values: &[Value]) -> Option<LogicalType> {
    let mut shape = None;
    for value in values {
        let (kind, view) = match value {
            Value::Null => continue,
            Value::List(list) => (ListKind::List, list.view()),
            Value::FixedSizeList(list) => (ListKind::FixedSizeList(i32::try_from(list.len()).ok()?), list.view()),
            _ => return None,
        };
        let width = match view {
            rtdb_types::ListView::F32(_) => 32,
            rtdb_types::ListView::F64(_) => 64,
            _ => return None,
        };
        match &shape {
            None => shape = Some((kind, width)),
            Some(known) if *known == (kind, width) => {}
            Some(_) => return None,
        }
    }
    shape?;
    Some(LogicalType::join_all(&values.iter().map(Value::logical_type).collect::<Vec<_>>()))
}

fn literal_cell(expression: &rtdb_plan::Expr) -> Option<&Value> {
    match expression { rtdb_plan::Expr::Literal(value) => Some(value), _ => None }
}

fn scalar_result_type<'a>(values: impl Iterator<Item = &'a Value>) -> Option<LogicalType> {
    let mut ty = None;
    let mut nullable = false;
    let mut remaining_variable_bytes = i32::MAX as usize;
    for value in values {
        match value {
            Value::Utf8(value) => remaining_variable_bytes = remaining_variable_bytes.checked_sub(value.len())?,
            Value::Keyword(value) => remaining_variable_bytes = remaining_variable_bytes.checked_sub(value.as_str().len())?,
            Value::Binary(value) => remaining_variable_bytes = remaining_variable_bytes.checked_sub(value.len())?,
            _ => {}
        }
        match value {
            Value::Null => nullable = true,
            Value::Bool(_) | Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_)
            | Value::F16(_) | Value::F32(_) | Value::F64(_) | Value::Utf8(_) | Value::Keyword(_) | Value::Binary(_)
            | Value::Uuid(_) | Value::FixedSizeBinary(_) | Value::Timestamp { .. }
            | Value::DateDay(_) | Value::DateMilli(_) | Value::Duration { .. }
            | Value::Oid(_) | Value::RegClass(_) | Value::RegProc(_) => {
                let next = value.logical_type();
                match &ty {
                    Some(ty) if *ty != next => return None,
                    None => ty = Some(next),
                    _ => {}
                }
            }
            _ => return None,
        }
    }
    Some(ty.map_or(LogicalType::Null, |ty: LogicalType| if nullable { ty.maybe() } else { ty }))
}

/// Column-at-a-time projection for input columns, scalar constants, `+ - *` over 64-bit integers and floats, records of
/// those, `COALESCE`, casts to a column's own type and row copies; `Ok(None)` where the evaluator must compute it,
/// including any integer overflow, so its error is raised.
pub(crate) mod projection {
    use std::sync::Arc;
    use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, ListArray, StructArray, make_array};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_schema::{DataType, Field as ArrowField};
    use rtdb_plan::{BinaryOp, CastTarget, Expr, Schema};
    use rtdb_types::{LogicalType, Result, ScalarType, StructFields, Value};
    use super::{Column, ColumnBatch, MemoryBudget, MemoryReservation};

    #[derive(Clone, Copy)]
    enum Number { Int(i64), Float(f64) }
    enum Numbers<'a> { Int(&'a Int64Array), Float(&'a Float64Array), Constant(Option<Number>) }
    impl Numbers<'_> {
        fn get(&self, row: usize) -> Option<Number> {
            match self {
                Self::Int(array) => array.is_valid(row).then(|| Number::Int(array.value(row))),
                Self::Float(array) => array.is_valid(row).then(|| Number::Float(array.value(row))),
                Self::Constant(number) => *number,
            }
        }
        fn floating(&self) -> Option<bool> { match self { Self::Int(_) => Some(false), Self::Float(_) => Some(true), Self::Constant(Some(Number::Float(_))) => Some(true), Self::Constant(Some(Number::Int(_))) => Some(false), Self::Constant(None) => None } }
    }
    fn constant(value: &Value) -> Option<Option<Number>> {
        match value {
            Value::Null => Some(None),
            Value::I8(value) => Some(Some(Number::Int(i64::from(*value)))), Value::I16(value) => Some(Some(Number::Int(i64::from(*value)))),
            Value::I32(value) => Some(Some(Number::Int(i64::from(*value)))), Value::I64(value) => Some(Some(Number::Int(*value))),
            Value::F64(value) => Some(Some(Number::Float(*value))),
            _ => None,
        }
    }
    #[derive(Clone, Copy)]
    enum Parameters<'a> { Values(rtdb_expr::Parameters<'a>), Columns(&'a [Column]) }

    fn numbers<'a>(expr: &Expr, schema: &Schema, columns: &'a [Column], len: usize, computed: &'a mut Option<Column>, parameters: Parameters<'a>) -> Result<Option<Numbers<'a>>> {
        let plain = |logical: &Option<LogicalType>, field: &ArrowField, expected: ScalarType| match logical { Some(logical) => super::plain_scalar(logical) == Some(&expected), None => field.metadata().is_empty() };
        let array_numbers = |column: &'a Column| -> Option<Numbers<'a>> {
            let Column::Arrow { array, field, logical } = column else { return None };
            match array.data_type() {
                DataType::Int64 if plain(logical, field, ScalarType::I64) => array.as_any().downcast_ref().map(Numbers::Int),
                DataType::Float64 if plain(logical, field, ScalarType::F64) => array.as_any().downcast_ref().map(Numbers::Float),
                _ => None,
            }
        };
        Ok(match expr {
            Expr::Column { id, .. } => schema.position(*id).and_then(|position| array_numbers(&columns[position])),
            Expr::Literal(value) => constant(value).map(Numbers::Constant),
            Expr::Parameter { index, .. } => match parameters {
                Parameters::Values(values) => values.known(*index).and_then(constant).map(Numbers::Constant),
                Parameters::Columns(columns) => columns.get(*index).and_then(array_numbers),
            },
            Expr::Binary { .. } => {
                *computed = arithmetic(expr, schema, columns, len, parameters)?;
                computed.as_ref().and_then(array_numbers)
            }
            _ => None,
        })
    }
    fn arithmetic(expr: &Expr, schema: &Schema, columns: &[Column], len: usize, parameters: Parameters<'_>) -> Result<Option<Column>> {
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || arithmetic_at(expr, schema, columns, len, parameters))
    }

    fn arithmetic_at(expr: &Expr, schema: &Schema, columns: &[Column], len: usize, parameters: Parameters<'_>) -> Result<Option<Column>> {
        let Expr::Binary { op: op @ (BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply), left, right } = expr else { return Ok(None) };
        let (mut left_computed, mut right_computed) = (None, None);
        let Some(left) = numbers(left, schema, columns, len, &mut left_computed, parameters)? else { return Ok(None) };
        let Some(right) = numbers(right, schema, columns, len, &mut right_computed, parameters)? else { return Ok(None) };
        if matches!(left, Numbers::Constant(_)) && matches!(right, Numbers::Constant(_)) { return Ok(None); }
        let floating = match (left.floating(), right.floating()) { (Some(a), Some(b)) => a || b, (Some(a), None) | (None, Some(a)) => a, (None, None) => return Ok(None) };
        let float = |number: Number| match number { Number::Int(value) => value as f64, Number::Float(value) => value };
        let (array, scalar): (ArrayRef, ScalarType) = if floating {
            let values: Float64Array = (0..len).map(|row| match (left.get(row), right.get(row)) {
                (Some(a), Some(b)) => Some(match op { BinaryOp::Add => float(a) + float(b), BinaryOp::Subtract => float(a) - float(b), _ => float(a) * float(b) }),
                _ => None,
            }).collect();
            (Arc::new(values), ScalarType::F64)
        } else {
            let mut values = Vec::with_capacity(len);
            for row in 0..len {
                values.push(match (left.get(row), right.get(row)) {
                    (Some(Number::Int(a)), Some(Number::Int(b))) => match match op { BinaryOp::Add => a.checked_add(b), BinaryOp::Subtract => a.checked_sub(b), _ => a.checked_mul(b) } {
                        Some(value) => Some(value),
                        None => return Ok(None),
                    },
                    _ => None,
                });
            }
            (Arc::new(Int64Array::from(values)), ScalarType::I64)
        };
        let field = Arc::new(ArrowField::new("value", array.data_type().clone(), true));
        Ok(Some(Column::Arrow { array, field, logical: Some(LogicalType::Scalar(scalar)) }))
    }

    pub(crate) fn bound_column(expr: &Expr, schema: &Schema, columns: &[Column], parameters: &[Column]) -> Result<Option<Column>> {
        match expr {
            Expr::Column { id, .. } => Ok(schema.position(*id).map(|position| columns[position].duplicate())),
            Expr::Parameter { index, .. } => Ok(parameters.get(*index).map(Column::duplicate)),
            _ => arithmetic(expr, schema, columns, 1, Parameters::Columns(parameters)),
        }
    }

    /// Whether an expression has a shape the kernel may compute; its cells' types decide per batch.
    pub(crate) fn shaped(expr: &Expr) -> bool {
        match expr {
            Expr::Column { .. } | Expr::Parameter { .. } | Expr::RowCopy(_) => true,
            Expr::Literal(value) => literal(value),
            Expr::Binary { op: BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply, left, right } => shaped(left) && shaped(right),
            Expr::Struct(fields) => fields.iter().all(|(_, field)| shaped(field)),
            Expr::Field { expr, .. } => matches!(expr.as_ref(), Expr::Column { .. }),
            Expr::Coalesce(args) => args.iter().all(shaped),
            Expr::Call { name, args, .. } if name == "coalesce" => args.iter().all(shaped),
            Expr::Call { name, args, .. } if name == "_patch" => args.iter().all(|arg| matches!(arg, Expr::Column { .. })),
            Expr::Call { name, args, .. } if clock(name, args) => true,
            call if super::search::shaped(call) => true,
            Expr::Cast { expr: inner, target: CastTarget::Fixed(_) } if shaped(inner) => true,
            expr => crate::vector::lowerable(expr),
        }
    }
    fn literal(value: &Value) -> bool { constant(value).is_some() || matches!(value, Value::Utf8(_) | Value::Bool(_) | Value::Timestamp { .. }) }
    /// The query clock, which `BatchExpressions` reads once and hands the kernels as a literal.
    pub(crate) fn clock(name: &str, args: &[Expr]) -> bool { args.is_empty() && matches!(name, "current_timestamp" | "current-timestamp" | "now") }
    /// Bounds what `column` allocates for `expr`, or `None` where it would not compute it.
    pub(crate) fn bound(expr: &Expr, schema: &Schema, batch: &ColumnBatch) -> Result<Option<usize>> {
        let rows = batch.len;
        let per_row = |bytes: usize| rows.checked_mul(bytes).and_then(|total| total.checked_add(256)).ok_or_else(super::size_error);
        Ok(Some(match expr {
            Expr::Column { id, .. } => match schema.position(*id) { Some(position) => batch.columns[position].retained_bytes()?, None => return Ok(None) },
            Expr::Literal(value) if literal(value) => super::repeated_cell_bound(value, rows)?,
            Expr::Parameter { .. } => 0,
            // The checked path builds `Option<i64>` cells before the array, and each operand may be computed too.
            Expr::Binary { op: BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply, left, right } => {
                let (Some(left), Some(right)) = (bound(left, schema, batch)?, bound(right, schema, batch)?) else { return Ok(None) };
                per_row(32)?.checked_add(left).and_then(|bytes| bytes.checked_add(right)).ok_or_else(super::size_error)?
            }
            Expr::List(items) => {
                let Some((arrays, logical)) = list_columns(items, schema, batch) else { return Ok(None); };
                let bytes = arrays.iter().try_fold(4096usize, |bytes, array| {
                    let visible = array.to_data().get_slice_memory_size().unwrap_or_else(|_| array.get_buffer_memory_size());
                    visible.checked_mul(4).and_then(|size| bytes.checked_add(size)).ok_or_else(super::size_error)
                })?;
                let metadata = logical.retained_bytes()?;
                items.len().checked_mul(64).and_then(|bytes| bytes.checked_add(8))
                    .and_then(|bytes| bytes.checked_mul(rows))
                    .and_then(|scratch| scratch.checked_add(bytes))
                    .and_then(|bytes| bytes.checked_add(metadata.checked_mul(8)?))
                    .ok_or_else(super::size_error)?
            }
            Expr::Field { expr, name, .. } => {
                let Some((_, array, field, logical)) = struct_field(expr, name, schema, batch) else { return Ok(None); };
                per_row(1)?.checked_add(array.get_array_memory_size())
                    .and_then(|bytes| bytes.checked_add(field.size().checked_mul(4)?))
                    .and_then(|bytes| bytes.checked_add(logical.retained_bytes().ok()?))
                    .ok_or_else(super::size_error)?
            }
            Expr::Struct(fields) => {
                if direct_struct(fields, schema, batch) {
                    return fields.iter().try_fold(4096usize, |bytes, (name, expression)| {
                        let Some(child) = bound(expression, schema, batch)? else { return Err(super::size_error()); };
                        bytes.checked_add(child).and_then(|bytes| bytes.checked_add(name.len().checked_mul(8)?))
                            .ok_or_else(super::size_error)
                    }).map(Some);
                }
                let mut total = per_row(std::mem::size_of::<Value>().checked_add(256).ok_or_else(super::size_error)?)?;
                for (name, field) in fields {
                    let Some(child) = bound(field, schema, batch)? else { return Ok(None) };
                    let cells = match field {
                        Expr::Column { id, .. } => match schema.position(*id) { Some(position) => (0..rows).try_fold(0usize, |sum, row| sum.checked_add(batch.columns[position].bound(row)?).ok_or_else(super::size_error))?, None => return Ok(None) },
                        _ => child,
                    };
                    total = per_row(name.len().checked_add(std::mem::size_of::<Value>() + std::mem::size_of::<String>() + 64).ok_or_else(super::size_error)?)?
                        .checked_add(child).and_then(|bytes| bytes.checked_add(cells)).and_then(|bytes| bytes.checked_add(total)).ok_or_else(super::size_error)?;
                }
                total
            }
            Expr::Coalesce(args) => return coalesce_bound(args, schema, batch),
            Expr::Call { name, args, .. } if name == "coalesce" => return coalesce_bound(args, schema, batch),
            Expr::Cast { expr: inner, target: CastTarget::Fixed(_) } => return bound(inner, schema, batch),
            Expr::RowCopy(fields) => {
                if row_copy_compatible(fields, schema, batch) {
                    let bytes = fields.iter().try_fold(4096usize, |bytes, field| {
                        let position = schema.position(field.id).expect("Row-copy field is in the input");
                        let retained = batch.columns[position].retained_bytes()?;
                        let metadata = field.data_type.retained_bytes()?.checked_add(field.name.len())
                            .and_then(|bytes| bytes.checked_mul(8)).ok_or_else(super::size_error)?;
                        bytes.checked_add(retained).and_then(|bytes| bytes.checked_add(metadata)).ok_or_else(super::size_error)
                    })?;
                    return Ok(Some(bytes));
                }
                let mut total = per_row(std::mem::size_of::<Value>().checked_add(256).ok_or_else(super::size_error)?)?;
                for field in fields {
                    let Some(position) = schema.position(field.id) else { return Ok(None) };
                    let cells = (0..rows).try_fold(0usize, |sum, row| sum.checked_add(batch.columns[position].bound(row)?).ok_or_else(super::size_error))?;
                    total = per_row(field.name.len().checked_add(std::mem::size_of::<Value>() + std::mem::size_of::<String>() + 64).ok_or_else(super::size_error)?)?
                        .checked_add(cells).and_then(|bytes| bytes.checked_add(total)).ok_or_else(super::size_error)?;
                }
                total
            }
            _ => return Ok(None),
        }))
    }
    /// The computed arguments, and one cell of each per row.
    fn coalesce_bound(args: &[Expr], schema: &Schema, batch: &ColumnBatch) -> Result<Option<usize>> {
        let rows = batch.len;
        let mut total = rows.checked_mul(std::mem::size_of::<Value>() + 64).and_then(|bytes| bytes.checked_add(256)).ok_or_else(super::size_error)?;
        for arg in args {
            let Some(computed) = bound(arg, schema, batch)? else { return Ok(None) };
            let cells = match arg {
                Expr::Column { id, .. } => match schema.position(*id) { Some(position) => (0..rows).try_fold(0usize, |sum, row| sum.checked_add(batch.columns[position].bound(row)?).ok_or_else(super::size_error))?, None => return Ok(None) },
                _ => computed,
            };
            total = total.checked_add(computed).and_then(|bytes| bytes.checked_add(cells)).ok_or_else(super::size_error)?;
        }
        Ok(Some(total))
    }
    pub(crate) fn column(expr: &Expr, schema: &Schema, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>) -> Result<Option<Column>> {
        match expr {
            Expr::Column { id, .. } => {
                let Some(position) = schema.position(*id) else { return Ok(None) };
                Ok(Some(match &batch.columns[position] {
                    column @ (Column::Arrow { .. } | Column::Selected(_)) => column.duplicate(),
                    Column::Values(values) => Column::Values(values.iter().map(|value| { let mut value = value.clone(); value.admit_expression_dates()?; Ok(value) }).collect::<Result<_>>()?),
                }))
            }
            Expr::Literal(value) if literal(value) => super::repeat_cell(value, batch.len).map(Some),
            Expr::Binary { .. } => arithmetic(expr, schema, &batch.columns, batch.len, Parameters::Values(parameters)),
            Expr::List(items) => {
                let Some((arrays, logical)) = list_columns(items, schema, batch) else { return Ok(None); };
                let child = Arc::new(rtdb_columnar::to_field("item", &logical)?);
                let picked = (0..batch.len).flat_map(|row| (0..arrays.len()).map(move |column| (column, row))).collect::<Vec<_>>();
                let Ok(values) = rtdb_columnar::copy_selected_cells(&arrays, &picked) else { return Ok(None); };
                let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(arrays.len(), batch.len));
                let array = Arc::new(ListArray::try_new(child, offsets, values, None).map_err(|error| crate::invalid("column-list", error))?);
                let field = Arc::new(ArrowField::new("value", array.data_type().clone(), false));
                Ok(Some(Column::Arrow { array, field, logical: Some(LogicalType::list(logical)) }))
            }
            Expr::Field { expr, name, .. } => {
                let Some((parent, child, field, logical)) = struct_field(expr, name, schema, batch) else { return Ok(None); };
                if parent.null_count() == 0 || child.data_type() == &DataType::Null {
                    return Ok(Some(Column::Arrow { array: Arc::clone(child), field: Arc::clone(field), logical: Some(logical.clone()) }));
                }
                if matches!(child.data_type(), DataType::Union(..) | DataType::RunEndEncoded(..)) { return Ok(None); }
                let nulls = NullBuffer::union(parent.nulls(), child.nulls());
                let Ok(data) = child.to_data().into_builder().nulls(nulls).build() else { return Ok(None); };
                let array = make_array(data);
                let field = Arc::new(field.as_ref().clone().with_nullable(true));
                Ok(Some(Column::Arrow { array, field, logical: Some(logical.clone().maybe()) }))
            }
            Expr::Struct(fields) => {
                if direct_struct(fields, schema, batch) {
                    let columns = fields.iter().map(|(name, expr)| {
                        let Expr::Column { id, .. } = expr else { unreachable!("The struct's inputs are columns"); };
                        let position = schema.position(*id).expect("The struct's input field was checked");
                        (name.as_str(), &batch.columns[position])
                    });
                    return Ok(record_from_columns(fields.len(), columns));
                }
                let mut columns = Vec::with_capacity(fields.len());
                for (_, field) in fields {
                    let Some(column) = column(field, schema, batch, parameters)? else { return Ok(None) };
                    columns.push(column);
                }
                let mut values = Vec::with_capacity(batch.len);
                for row in 0..batch.len {
                    let mut document = StructFields::new();
                    for ((name, _), column) in fields.iter().zip(&columns) {
                        let mut value = column.value(row)?;
                        value.admit_box_date()?;
                        document.insert(name.clone(), value);
                    }
                    let mut value = Value::Struct(document);
                    value.admit_expression_dates()?;
                    rtdb_expr::validate_projection_value(&value)?;
                    values.push(value);
                }
                Ok(Some(Column::Values(values)))
            }
            Expr::Coalesce(args) => coalesce(args, schema, batch, parameters),
            Expr::Call { name, args, .. } if name == "coalesce" => coalesce(args, schema, batch, parameters),
            Expr::Cast { expr: inner, target: CastTarget::Fixed(target) } => {
                let Some(column) = column(inner, schema, batch, parameters)? else { return Ok(None) };
                // A cast to the type its cells already have returns each cell as it is.
                let Column::Arrow { logical: Some(logical), .. } = &column else { return Ok(None) };
                let target = target.unencoded();
                let target = match &target { LogicalType::Maybe(inner) => inner.as_ref(), target => target };
                let identity = super::plain_scalar(logical).is_some_and(|scalar| super::copyable(scalar) && *target == LogicalType::Scalar(scalar.clone()));
                Ok(identity.then_some(column))
            }
            Expr::RowCopy(fields) => row_copy(fields, schema, batch),
            _ => Ok(None),
        }
    }

    fn struct_field<'a>(expr: &Expr, name: &str, schema: &Schema, batch: &'a ColumnBatch)
        -> Option<(&'a StructArray, &'a ArrayRef, &'a Arc<ArrowField>, &'a LogicalType)> {
        let Expr::Column { id, .. } = expr else { return None; };
        let Column::Arrow { array, field, logical: Some(logical) } = &batch.columns[schema.position(*id)?] else { return None; };
        if !super::stable_read_layout(logical, field) { return None; }
        let logical = match logical { LogicalType::Maybe(inner) => inner.as_ref(), logical => logical };
        let LogicalType::Struct(types) = logical else { return None; };
        let logical = types.get(name)?;
        if *logical == LogicalType::Nothing { return None; }
        let parent = array.as_any().downcast_ref::<StructArray>()?;
        let position = parent.fields().iter().position(|field| field.name() == name)?;
        Some((parent, parent.column(position), &parent.fields()[position], logical))
    }

    fn list_columns<'a>(items: &[Expr], schema: &Schema, batch: &'a ColumnBatch) -> Option<(Vec<&'a dyn Array>, LogicalType)> {
        if items.len().checked_mul(batch.len)? > i32::MAX as usize { return None; }
        let mut arrays: Vec<&dyn Array> = Vec::with_capacity(items.len());
        let mut element = None;
        let mut nullable = false;
        for item in items {
            let Expr::Column { id, .. } = item else { return None; };
            let Column::Arrow { array, field, logical: Some(logical) } = &batch.columns[schema.position(*id)?] else { return None; };
            if *logical == LogicalType::Nothing || !super::stable_read_layout(logical, field) || !super::can_interleave_layout(array.as_ref(), true) {
                return None;
            }
            let ty = match logical { LogicalType::Maybe(ty) => ty.as_ref(), logical => logical };
            if element.is_some_and(|previous| previous != ty)
                || arrays.first().is_some_and(|previous| previous.data_type() != array.data_type()) { return None; }
            element = Some(ty);
            nullable |= logical.nullable() || array.logical_null_count() != 0;
            arrays.push(array.as_ref());
        }
        let logical = element?.clone();
        let logical = if nullable { logical.maybe() } else { logical };
        let child = rtdb_columnar::to_field("item", &logical).ok()?;
        (arrays.first()?.data_type() == child.data_type()).then_some((arrays, logical))
    }

    pub(crate) fn coalesce_vectors(
        expr: &Expr, schema: &Schema, batch: &ColumnBatch, memory: &Arc<MemoryBudget>,
    ) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
        let args = match expr {
            Expr::Coalesce(args) => args,
            Expr::Call { name, args, .. } if name == "coalesce" => args,
            _ => return Ok(None),
        };
        if batch.len == 0 { return Ok(None); }
        let bytes = args.len().checked_mul(64).and_then(|bytes| bytes.checked_add(256)).ok_or_else(super::size_error)?;
        let Ok(_inputs) = memory.reserve(bytes) else { return Ok(None); };
        let mut inputs = Vec::with_capacity(args.len());
        let layout = |field: &ArrowField, logical: Option<&LogicalType>| {
            if !logical.is_some_and(|logical| super::stable_read_layout(logical, field)) { return false; }
            match field.data_type() {
                DataType::Null => field.is_nullable(),
                DataType::List(child) | DataType::FixedSizeList(child, _) if field.metadata().is_empty() && child.metadata().is_empty() => {
                    matches!(child.data_type(), DataType::Float32 | DataType::Float64)
                        || rtdb_types::ListValue::supports_float_union(child.data_type())
                }
                _ => false,
            }
        };
        for arg in args {
            let position = match arg {
                Expr::Literal(Value::Null | Value::Undefined) => continue,
                Expr::Column { id, .. } => match schema.position(*id) { Some(position) => position, None => return Ok(None) },
                _ => return Ok(None),
            };
            let column = &batch.columns[position];
            let compatible = match column {
                Column::Arrow { field, logical, .. } => layout(field, logical.as_ref()),
                Column::Selected(column) => column.source_layouts().all(|(_, field, logical)| layout(field, logical)),
                Column::Values(_) => false,
            };
            if !compatible { return Ok(None); }
            inputs.push((column, batch.column_reservation(position)));
        }
        if inputs.is_empty() { return Ok(None); }
        let picks = (0..batch.len).map(|row| inputs.iter().find_map(|&(column, charge)| {
            let (array, field, _, offset) = column.arrow_cell(row)??;
            (super::arrow_cell_is_null(field, array, offset) == Some(false)).then_some((column, charge, row))
        }));
        let selection = super::take_arrow_rows(picks, batch.len, true, memory).and_then(|(column, mut charge)| {
            let bytes = column.retained_bytes()?;
            let lease = Arc::get_mut(&mut charge).expect("A vector selection owns its reservation");
            if bytes <= lease.bytes() { lease.shrink_to(bytes)?; } else { lease.grow_to(bytes)?; }
            Ok((column, charge))
        });
        match selection {
            Err(error) if error.code == "rtdb/memory-budget-exceeded" => Ok(None),
            result => result.map(Some),
        }
    }

    /// The first non-null argument per row. An argument the kernels can't compute, or one that raises an error the
    /// evaluator might never reach, leaves the whole call to the evaluator.
    fn coalesce(args: &[Expr], schema: &Schema, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>) -> Result<Option<Column>> {
        enum Arg { Constant(Value), Cells(Column) }
        let mut computed = Vec::with_capacity(args.len());
        for arg in args {
            match arg {
                Expr::Literal(Value::Null | Value::Undefined) => continue,
                Expr::Literal(value) => { computed.push(Arg::Constant(value.clone())); break; }
                arg => match column(arg, schema, batch, parameters) { Ok(Some(column)) => computed.push(Arg::Cells(column)), _ => return Ok(None) },
            }
        }
        let Some(last) = computed.iter().rposition(|arg| matches!(arg, Arg::Cells(_))) else { return Ok(None) };
        // Arguments of one plain scalar layout are interleaved in bulk, a constant as a one-cell array of that layout.
        let layout = computed.iter().find_map(|arg| match arg { Arg::Cells(Column::Arrow { array, logical: Some(logical), .. }) => Some((array.data_type().clone(), super::plain_scalar(logical)?.clone())), _ => None });
        if let Some((data_type, scalar)) = layout.filter(|(data_type, scalar)| super::copyable(scalar) && super::scalar_layout(data_type) && *data_type != DataType::Null) {
            let logical = LogicalType::Scalar(scalar);
            let mut arrays: Vec<ArrayRef> = Vec::with_capacity(computed.len());
            for arg in &computed {
                let array = match arg {
                    Arg::Cells(Column::Arrow { array, logical: Some(other), .. }) if array.data_type() == &data_type && super::plain_scalar(other) == super::plain_scalar(&logical) => Some(Arc::clone(array)),
                    Arg::Constant(value) if value.logical_type() == logical => rtdb_columnar::copy_array(&logical, std::slice::from_ref(value)).ok().filter(|array| array.data_type() == &data_type),
                    _ => None,
                };
                match array { Some(array) => arrays.push(array), None => break }
            }
            if arrays.len() == computed.len() {
                let picks = (0..batch.len).map(|row| match computed.iter().zip(&arrays).position(|(arg, array)| matches!(arg, Arg::Constant(_)) || array.is_valid(row)) {
                    Some(index) if matches!(computed[index], Arg::Constant(_)) => (index, 0),
                    Some(index) => (index, row),
                    None => (last, row),
                }).collect::<Vec<_>>();
                let sources: Vec<&dyn Array> = arrays.iter().map(|array| array.as_ref()).collect();
                let array = arrow_select::interleave::interleave(&sources, &picks).map_err(|error| crate::invalid("column-coalesce", error))?;
                let field = Arc::new(ArrowField::new("value", data_type, true));
                return Ok(Some(Column::Arrow { array, field, logical: Some(logical) }));
            }
        }
        let mut values = Vec::with_capacity(batch.len);
        for row in 0..batch.len {
            let mut value = Value::Null;
            for arg in &computed {
                let cell = match arg { Arg::Constant(value) => value.clone(), Arg::Cells(column) => match column.value(row) { Ok(cell) => cell, Err(_) => return Ok(None) } };
                if !matches!(cell, Value::Null | Value::Undefined) { value = cell; break; }
            }
            if value.admit_expression_dates().is_err() || rtdb_expr::validate_projection_value(&value).is_err() { return Ok(None); }
            values.push(value);
        }
        Ok(Some(Column::Values(values)))
    }

    fn direct_struct(fields: &[(String, Expr)], schema: &Schema, batch: &ColumnBatch) -> bool {
        !fields.is_empty() && fields.iter().enumerate().all(|(index, (name, expression))| {
            if fields[..index].iter().any(|(key, _)| key == name) { return false; }
            let Expr::Column { id, .. } = expression else { return false; };
            let Some(position) = schema.position(*id) else { return false; };
            let Column::Arrow { field, logical: Some(logical), .. } = &batch.columns[position] else { return false; };
            *logical != LogicalType::Nothing && super::stable_read_layout(logical, field)
                && !matches!(field.data_type(), DataType::Union(..) | DataType::RunEndEncoded(..))
        })
    }

    fn record_from_columns<'a>(width: usize, columns: impl Iterator<Item = (&'a str, &'a Column)>) -> Option<Column> {
        let mut arrow_fields = Vec::with_capacity(width);
        let mut arrays = Vec::with_capacity(width);
        let mut types = StructFields::new();
        for (name, column) in columns {
            let Column::Arrow { array, field: source, logical: Some(logical) } = column else { return None; };
            let child = ArrowField::new(name, array.data_type().clone(), source.is_nullable() || array.null_count() != 0)
                .with_metadata(source.metadata().clone());
            arrow_fields.push(Arc::new(child));
            arrays.push(Arc::clone(array));
            types.insert(name.to_owned(), logical.clone());
        }
        let array = StructArray::try_new(arrow_fields.into(), arrays, None).ok()?;
        let field = Arc::new(ArrowField::new("value", array.data_type().clone(), false));
        Some(Column::Arrow { array: Arc::new(array), field, logical: Some(LogicalType::Struct(types)) })
    }

    fn row_copy_compatible(fields: &[rtdb_plan::Field], schema: &Schema, batch: &ColumnBatch) -> bool {
        !fields.is_empty() && fields.iter().enumerate().all(|(index, field)| {
            if fields[..index].iter().any(|other| other.name == field.name) { return false; }
            let Some(column) = schema.position(field.id).and_then(|position| batch.columns.get(position)) else { return false; };
            let Column::Arrow { array, field: source, logical: Some(logical) } = column else { return false; };
            let scalar = super::plain_scalar(logical).is_some() && super::scalar_layout(array.data_type()) && array.data_type() != &DataType::Null;
            let nested = matches!(array.data_type(), DataType::Struct(_) | DataType::List(_) | DataType::FixedSizeList(..))
                && super::stable_read_layout(logical, source)
                && rtdb_columnar::to_field(source.name(), logical).is_ok_and(|field| field.data_type() == array.data_type());
            scalar || nested
        })
    }

    fn row_copy(fields: &[rtdb_plan::Field], schema: &Schema, batch: &ColumnBatch) -> Result<Option<Column>> {
        let Some(columns) = fields.iter().map(|field| schema.position(field.id).map(|position| &batch.columns[position])).collect::<Option<Vec<_>>>() else { return Ok(None) };
        if row_copy_compatible(fields, schema, batch) {
            let columns = fields.iter().zip(&columns).map(|(field, column)| (field.name.as_str(), *column));
            return Ok(record_from_columns(fields.len(), columns));
        }
        let mut values = Vec::with_capacity(batch.len);
        for row in 0..batch.len {
            let mut document = StructFields::new();
            for (field, column) in fields.iter().zip(&columns) {
                match column.value(row) { Ok(value) => { document.insert(field.name.clone(), value); } Err(_) => return Ok(None) }
            }
            let mut value = Value::Struct(document);
            if value.admit_expression_dates().is_err() { return Ok(None); }
            values.push(value);
        }
        Ok(Some(Column::Values(values)))
    }
}

#[test]
fn vector_coalesce_uses_arrow_validity_and_retains_both_layouts() {
    let memory = MemoryBudget::new(1 << 20);
    let left = Value::FixedSizeList(vec![Value::F32(-0.0), Value::Null].into());
    let right = Value::list(vec![Value::F64(1.0)]);
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for (id, name, value, cells) in [
        (0, "left", &left, [left.clone(), Value::Null, Value::Null]),
        (1, "right", &right, [right.clone(), right.clone(), Value::Null]),
    ] {
        let logical = value.logical_type().maybe();
        let field = Arc::new(rtdb_columnar::to_field(name, &logical).unwrap());
        let array = rtdb_columnar::copy_array(&logical, &cells).unwrap();
        columns.push(Column::Arrow { array, field, logical: Some(logical.clone()) });
        fields.push(Field { id: rtdb_plan::ColumnId(id), name: name.into(), qualifier: None,
            qualified_only: false, unnamed: false, data_type: logical });
    }
    let expr = rtdb_plan::Expr::Coalesce(fields.iter().map(Field::expression).collect());
    let schema = rtdb_plan::Schema::new(fields).unwrap();
    let batch = ColumnBatch::settled(columns, 3, memory.reserve(4096).unwrap()).unwrap();
    let (column, _charge) = projection::coalesce_vectors(&expr, &schema, &batch, &memory).unwrap().unwrap();
    for (row, expected) in [left, right].into_iter().enumerate() {
        let (input, ..) = batch.columns[row].arrow_cell(row).unwrap().unwrap();
        let (retained, ..) = column.arrow_cell(row).unwrap().unwrap();
        assert!(std::ptr::eq(retained, input));
        assert_eq!(column.value(row).unwrap(), expected);
    }
    assert_eq!(column.value(2).unwrap(), Value::Null);
}
