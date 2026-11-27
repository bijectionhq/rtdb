//! Column-at-a-time pages: a scan gathers the cells its visible rows resolve to into one column per output field,
//! so filters and aggregates can read whole columns, and rows are materialised only for operators that need them.
pub(crate) mod apply;
mod concat;
pub(crate) mod float_vector;
mod float_vectors;
pub(crate) mod functions;
mod patch;
pub(crate) mod periods;
pub(crate) mod search;
pub(crate) mod window;

use std::{borrow::Borrow, collections::{HashMap, HashSet}, ops::Range, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, ListArray, MapArray, NullArray, StructArray, UnionArray,
    builder::{BooleanBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder},
};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_schema::{DataType, Field as ArrowField};
use rtdb_columnar::{CellReadCheck, MemoryBudget, MemoryReservation, copy_selected_cells, read_execution_value, read_execution_value_as};
use rtdb_index::IndexBatch;
use rtdb_plan::{CastTarget, Expr, Field, UnaryOp};
use rtdb_temporal::ResolvedRow;
use rtdb_types::{ListKind, LogicalType, Result, ScalarType, Timestamp, Value};

use crate::{invalid, pages::OwnedPage};
use crate::trace::{OperatorCounter, operator_evidence_enabled};
#[cfg(test)]
use crate::{cursor::OwnedRow, pages::PageBuilder};

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
type FragmentVector<'a> = (&'a ArrayRef, &'a Arc<ArrowField>, &'a Option<LogicalType>, &'a Arc<MemoryReservation>);

#[derive(Clone, Copy)]
pub(crate) struct FundedArrowCell<'a> {
    pub array: &'a ArrayRef,
    pub field: &'a Arc<ArrowField>,
    pub logical: Option<&'a LogicalType>,
    pub owner: &'a Arc<MemoryReservation>,
    pub row: usize,
}

#[derive(Clone)]
pub(crate) struct SelectedColumn {
    sources: Arc<Vec<Option<StoredVector>>>,
    cells: SelectedRows,
}

#[derive(Clone)]
enum SelectedRows {
    Picks(Arc<Vec<(usize, usize)>>),
    Span(Range<usize>),
    Apply(apply::ApplySelection),
    Expanded {
        expansion: RowExpansion,
        rows: Range<usize>,
        input: Option<Arc<ExpansionInput>>,
    },
}

struct ExpansionInput {
    rows: SelectedRows,
    owner: Arc<MemoryReservation>,
}

#[derive(Clone)]
struct JoinProbeRows<'a> {
    spans: crate::batch_join::ProbeRunSpans<'a>,
    input: Option<&'a ExpansionInput>,
    current: std::iter::RepeatN<Option<(usize, usize)>>,
}

impl Iterator for JoinProbeRows<'_> {
    type Item = Option<(usize, usize)>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(cell) = self.current.next() { return Some(cell); }
        let (row, count) = self.spans.next()?;
        let cell = self.input.map_or(Some((0, row)), |input| input.rows.get(row));
        self.current = std::iter::repeat_n(cell, count - 1);
        Some(cell)
    }
}

#[derive(Clone)]
enum RowExpansion {
    ApplyOuter(Arc<apply::ApplySpans>),
    RepeatEach(usize),
    RepeatPage(usize),
    JoinProbe(Arc<crate::batch_join::ProbeRuns>),
    PageSpans(Arc<PageSpans>),
    WindowSpans(Arc<window::WindowSpans>),
    UnnestParents(Arc<UnnestRows>),
    UnnestGroups(Arc<UnnestRows>),
    UnnestChildren(Arc<UnnestRows>),
}

impl RowExpansion {
    fn get(&self, row: usize) -> (usize, usize) {
        match self {
            Self::ApplyOuter(spans) => (0, spans.outer(row)),
            Self::RepeatEach(width) => (0, row / width),
            Self::RepeatPage(width) => (0, row % width),
            Self::JoinProbe(runs) => (0, runs.input_row(row)),
            Self::PageSpans(spans) => spans.position(row),
            Self::WindowSpans(spans) => spans.position(row),
            Self::UnnestParents(runs) => (0, runs.groups[runs.group_at(row)].parent),
            Self::UnnestGroups(runs) => (0, runs.group_at(row)),
            Self::UnnestChildren(runs) => {
                let group = runs.group_at(row);
                runs.groups[group].child.map_or((0, 0), |(source, start)| {
                    (source, start + (row - runs.group_start(group)))
                })
            }
        }
    }

    fn contiguous(&self, rows: Range<usize>) -> Option<(usize, Range<usize>)> {
        let (source, start) = self.get(rows.start);
        match self {
            Self::ApplyOuter(spans) => return spans.outer_range(rows).map(|rows| (0, rows)),
            Self::RepeatEach(1) => {}
            Self::RepeatPage(width) if rows.len() <= width - start => {}
            Self::JoinProbe(runs) if runs.contiguous(&rows) => {}
            Self::PageSpans(spans) if rows.end <= spans.ends[source] => {}
            Self::WindowSpans(spans) if spans.contains(source, rows.end) => {}
            Self::UnnestChildren(runs) if source != 0 => {
                let first = runs.group_at(rows.start);
                let last = runs.group_at(rows.end - 1);
                for group in first + 1..=last {
                    let offset = runs.group_start(group) - rows.start;
                    if runs.groups[group].child != Some((source, start.checked_add(offset)?)) { return None; }
                }
            }
            _ if rows.len() == 1 => {}
            _ => return None,
        }
        Some((source, start..start.checked_add(rows.len())?))
    }

    fn owner(&self) -> Option<&Arc<MemoryReservation>> {
        match self {
            Self::ApplyOuter(spans) => Some(spans.reservation()),
            Self::PageSpans(spans) => Some(&spans.reservation),
            Self::WindowSpans(spans) => Some(spans.reservation()),
            Self::UnnestParents(runs) | Self::UnnestGroups(runs) | Self::UnnestChildren(runs) => Some(&runs.reservation),
            Self::JoinProbe(runs) => runs.owner(),
            Self::RepeatEach(_) | Self::RepeatPage(_) => None,
        }
    }
}

struct PageSpans {
    ends: Vec<usize>,
    reservation: Arc<MemoryReservation>,
}

impl PageSpans {
    fn position(&self, row: usize) -> (usize, usize) {
        let page = self.ends.partition_point(|&end| end <= row);
        let start = page.checked_sub(1).map_or(0, |previous| self.ends[previous]);
        (page, row - start)
    }
}

struct UnnestGroup {
    end: usize,
    parent: usize,
    child: Option<(usize, usize)>,
    ordinal_start: usize,
}

pub(crate) struct UnnestRows {
    groups: Vec<UnnestGroup>,
    reservation: Arc<MemoryReservation>,
}

impl UnnestRows {
    pub(crate) fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        Ok(Self { groups: Vec::new(), reservation: memory.reserve(256)? })
    }

    pub(crate) fn push(&mut self, parent: usize, child: Option<(usize, Range<usize>)>, ordinality: bool,
        memory: &Arc<MemoryBudget>) -> Result<()> {
        if ordinality && child.as_ref().is_some_and(|(_, rows)| i32::try_from(rows.len()).is_err()) {
            return Err(invalid("ordinality-overflow", "List ordinal exceeds signed32 bits"));
        }
        self.push_run(parent, child, 0, memory)
    }

    fn push_run(&mut self, parent: usize, child: Option<(usize, Range<usize>)>, ordinal_start: usize,
        memory: &Arc<MemoryBudget>) -> Result<()> {
        let count = child.as_ref().map_or(1, |(_, rows)| rows.len());
        let end = self.len().checked_add(count).ok_or_else(size_error)?;
        if self.groups.len() == self.groups.capacity() {
            let capacity = self.groups.capacity().checked_mul(2).ok_or_else(size_error)?.max(1);
            let bytes = capacity.checked_mul(size_of::<UnnestGroup>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
            let reservation = memory.reserve(bytes)?;
            self.groups.reserve_exact(capacity - self.groups.len());
            self.reservation = reservation;
        }
        self.groups.push(UnnestGroup { end, parent, child: child.map(|(source, rows)| (source, rows.start)), ordinal_start });
        Ok(())
    }

    fn len(&self) -> usize { self.groups.last().map_or(0, |group| group.end) }

    fn group_at(&self, row: usize) -> usize { self.groups.partition_point(|group| group.end <= row) }

    fn group_start(&self, group: usize) -> usize {
        group.checked_sub(1).map_or(0, |prev| self.groups[prev].end)
    }

    fn candidate_window(&self, rows: Range<usize>, memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let first_group = self.group_at(rows.start);
        let mut window = Self::new(memory)?;
        let mut group_start = self.group_start(first_group);
        for group in &self.groups[first_group..] {
            let start = rows.start.max(group_start) - group_start;
            let end = rows.end.min(group.end) - group_start;
            let (source, child_start) = group.child.expect("Candidate runs contain real list elements");
            window.push_run(group.parent, Some((source, child_start + start..child_start + end)),
                group.ordinal_start + start, memory)?;
            if group.end >= rows.end { break; }
            group_start = group.end;
        }
        Ok(Arc::new(window))
    }

    fn fill_unmatched_until(&mut self, end: usize, context: &crate::QueryContext) -> Result<()> {
        let start = self.groups.last().map_or(0, |last| last.parent + 1);
        for parent in start..end {
            context.check_cancelled_at(parent)?;
            self.push_run(parent, None, 0, &context.memory)?;
        }
        Ok(())
    }

    fn append_survivors(&mut self, candidates: &Self, selection: &PredicateSelection,
        context: &crate::QueryContext) -> Result<()> {
        let mut selected = selection.mask.values().set_slices().peekable();
        for (index, group) in candidates.groups.iter().enumerate() {
            let group_start = candidates.group_start(index);
            loop {
                let Some(&(start, end)) = selected.peek() else { return Ok(()); };
                if start >= group.end { break; }
                let start = start.max(group_start) - group_start;
                let stop = end.min(group.end) - group_start;
                let (source, child_start) = group.child.expect("Candidate runs contain real list elements");
                self.fill_unmatched_until(group.parent, context)?;
                self.push_surviving_run(group.parent, source, child_start + start..child_start + stop,
                    group.ordinal_start + start, &context.memory)?;
                if end > group.end { break; }
                let _ = selected.next();
            }
        }
        Ok(())
    }

    fn push_surviving_run(&mut self, parent: usize, source: usize, child: Range<usize>, ordinal_start: usize,
        memory: &Arc<MemoryBudget>) -> Result<()> {
        let previous_len = self.len() - self.group_start(self.groups.len().saturating_sub(1));
        if let Some(previous) = self.groups.last_mut()
            && previous.parent == parent
            && previous.child.is_some_and(|(previous_source, start)| previous_source == source && start + previous_len == child.start)
            && previous.ordinal_start + previous_len == ordinal_start
        {
            previous.end = previous.end.checked_add(child.len()).ok_or_else(size_error)?;
            return Ok(());
        }
        self.push_run(parent, Some((source, child)), ordinal_start, memory)
    }
}

const FILTERED_UNNEST_CANDIDATE_ROWS: usize = 1024;

#[derive(Default)]
pub(crate) struct FilteredUnnestCounts {
    pub candidates: u64,
    pub survivors: u64,
}

struct UnnestExpansion {
    runs: Arc<UnnestRows>,
    sources: Arc<Vec<Option<StoredVector>>>,
    custody: Arc<MemoryReservation>,
}

impl UnnestExpansion {
    fn elements(&self, runs: &Arc<UnnestRows>) -> Column {
        let cells = SelectedRows::Expanded {
            expansion: RowExpansion::UnnestChildren(Arc::clone(runs)), rows: 0..runs.len(), input: None,
        };
        Column::Selected(SelectedColumn { sources: Arc::clone(&self.sources), cells })
    }
}

impl SelectedRows {
    fn len(&self) -> usize {
        match self {
            Self::Picks(cells) => cells.len(),
            Self::Span(rows) | Self::Expanded { rows, .. } => rows.len(),
            Self::Apply(selection) => selection.len(),
        }
    }

    fn get(&self, row: usize) -> Option<(usize, usize)> {
        match self {
            Self::Picks(cells) => cells.get(row).copied(),
            Self::Span(rows) => (row < rows.len()).then(|| (0, rows.start + row)),
            Self::Apply(selection) => selection.get(row),
            Self::Expanded { expansion, rows, input } => {
                if row >= rows.len() { return None; }
                let cell = expansion.get(rows.start + row);
                input.as_ref().map_or(Some(cell), |input| input.rows.get(cell.1))
            }
        }
    }

    fn iter(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        let probe = self.join_probe_rows(0..self.len());
        let (picks, span, expanded, apply, window) = match self {
            Self::Picks(cells) => (cells.as_slice(), 0..0, 0..0, None, None),
            Self::Span(rows) => (&[][..], rows.clone(), 0..0, None, None),
            Self::Expanded { expansion: RowExpansion::WindowSpans(spans), rows, input: None } => {
                (&[][..], 0..0, 0..0, None, Some((spans, rows)))
            }
            Self::Expanded { expansion: RowExpansion::JoinProbe(_), .. } => (&[][..], 0..0, 0..0, None, None),
            Self::Expanded { rows, .. } => (&[][..], 0..0, 0..rows.len(), None, None),
            Self::Apply(selection) => (&[][..], 0..0, 0..0, Some(selection), None),
        };
        probe.into_iter().flatten().map(|cell| cell.expect("The expansion covers the selection"))
            .chain(picks.iter().copied())
            .chain(span.map(|row| (0, row)))
            .chain(expanded.map(|row| self.get(row).expect("The expansion covers the selection")))
            .chain(apply.into_iter().flat_map(|selection| selection.iter()))
            .chain(window.into_iter().flat_map(|(spans, rows)| spans.iter(rows.clone())))
    }

    fn join_probe_rows(&self, rows: Range<usize>) -> Option<JoinProbeRows<'_>> {
        let Self::Expanded { expansion: RowExpansion::JoinProbe(runs), rows: span, input } = self else {
            return None;
        };
        Some(JoinProbeRows {
            spans: runs.spans(span.start + rows.start..span.start + rows.end),
            input: input.as_deref(),
            current: std::iter::repeat_n(None, 0),
        })
    }

    fn slice(&self, rows: Range<usize>) -> Self {
        match self {
            Self::Picks(cells) => Self::Picks(Arc::new(cells[rows].to_vec())),
            Self::Span(span) => Self::Span(span.start + rows.start..span.start + rows.end),
            Self::Apply(selection) => Self::Apply(selection.slice(rows)),
            Self::Expanded { expansion, rows: span, input } => Self::Expanded {
                expansion: expansion.clone(), rows: span.start + rows.start..span.start + rows.end, input: input.clone(),
            },
        }
    }

    fn contiguous(&self, rows: Range<usize>) -> Option<(usize, Range<usize>)> {
        if rows.is_empty() || rows.end > self.len() { return None; }
        match self {
            Self::Span(span) => Some((0, span.start + rows.start..span.start + rows.end)),
            Self::Picks(cells) => {
                let (source, start) = cells[rows.start];
                let end = start.checked_add(rows.len())?;
                cells[rows].iter().copied().eq((start..end).map(|row| (source, row))).then_some((source, start..end))
            }
            Self::Apply(selection) => selection.contiguous(rows),
            Self::Expanded { expansion, rows: span, input } => {
                let (source, rows) = expansion.contiguous(span.start + rows.start..span.start + rows.end)?;
                match input { Some(input) => input.rows.contiguous(rows), None => Some((source, rows)) }
            }
        }
    }

    fn expansion_owners(&self) -> impl Iterator<Item = &Arc<MemoryReservation>> {
        std::iter::successors(Some(self), |rows| match *rows {
            Self::Expanded { input: Some(input), .. } => Some(&input.rows),
            _ => None,
        }).flat_map(|rows| match rows {
            Self::Expanded { expansion, input, .. } => [expansion.owner(), input.as_ref().map(|input| &input.owner)],
            _ => [None, None],
        }.into_iter().flatten().chain(match rows {
            Self::Apply(selection) => Some(selection),
            _ => None,
        }.into_iter().flat_map(|selection| selection.owners())))
    }
}

impl SelectedColumn {
    pub(crate) fn new(sources: Vec<Option<StoredVector>>, cells: Arc<Vec<(usize, usize)>>) -> Self {
        Self { sources: Arc::new(sources), cells: SelectedRows::Picks(cells) }
    }

    fn source(&self, row: usize) -> Result<Option<(&StoredVector, usize)>> {
        let (source, offset) = self.cells.get(row).ok_or_else(|| invalid("column-row", "Column row is outside its page"))?;
        Ok(self.sources[source].as_ref().map(|source| (source, offset)))
    }

    pub(crate) fn source_layouts(&self) -> impl Iterator<Item = (&dyn Array, &ArrowField, Option<&LogicalType>)> {
        self.sources.iter().flatten().map(|(array, field, logical, _)| (array.as_ref(), field.as_ref(), logical.as_ref()))
    }

    pub(crate) fn arrow_cells(&self) -> impl Iterator<Item = Option<(&dyn Array, &ArrowField, Option<&LogicalType>, usize)>> {
        self.cells.iter().map(|(source, row)| {
            self.sources[source].as_ref().map(|(array, field, logical, _)| {
                (array.as_ref(), field.as_ref(), logical.as_ref(), row)
            })
        })
    }

    fn select(self, selection: &PredicateSelection, span: Option<&Range<usize>>) -> Self {
        let cells = if let Some(rows) = span {
            self.cells.slice(rows.clone())
        } else {
            let mut cells = Vec::with_capacity(selection.count);
            cells.extend(selection.mask.values().set_indices().map(|row| {
                self.cells.get(row).expect("The column covers the selection")
            }));
            SelectedRows::Picks(Arc::new(cells))
        };
        Self { sources: self.sources, cells }
    }

    pub(crate) fn fragment_boundaries(&self, boundaries: &mut [bool]) {
        if self.sources.len() <= 1 || matches!(&self.cells, SelectedRows::Span(_)) { return; }
        let layout = |source: usize| self.sources[source].as_ref().map(|(_, field, logical, _)| (field, logical));
        let mut cells = self.cells.iter().map(|(source, _)| source);
        let Some(mut left) = cells.next() else { return; };
        for (boundary, right) in boundaries.iter_mut().skip(1).zip(cells) {
            if !*boundary && left != right && layout(left) != layout(right) {
                *boundary = true;
            }
            left = right;
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

    pub(crate) fn fragment_source(&self, rows: Range<usize>) -> Option<(ArrowVector, Range<usize>)> {
        let ((array, field, logical, _), rows) = self.contiguous_source(rows)?;
        Some(((Arc::clone(array), Arc::clone(field), logical.clone()), rows))
    }

    fn contiguous_source(&self, rows: Range<usize>) -> Option<(&StoredVector, Range<usize>)> {
        let (source, span) = self.cells.contiguous(rows)?;
        let vector = self.sources.get(source)?.as_ref()?;
        (span.end <= vector.0.len()).then_some((vector, span))
    }

    pub(crate) fn fragment_vector(&self, rows: Range<usize>, memory: &Arc<MemoryBudget>)
        -> Result<(ArrowVector, Arc<MemoryReservation>)> {
        self.uniform_fragment_vector(rows, memory)?.ok_or_else(|| invalid("fragment-layout", "Fragment rows differ in layout"))
    }

    /// As `fragment_vector`, `Ok(None)` where the rows' sources differ in layout.
    pub(crate) fn uniform_fragment_vector(&self, rows: Range<usize>, memory: &Arc<MemoryBudget>)
        -> Result<Option<(ArrowVector, Arc<MemoryReservation>)>> {
        let layout = |source: usize| self.sources[source].as_ref().map(|(_, field, logical, _)| (field, logical));
        let mut probe = self.cells.join_probe_rows(rows.clone());
        let cells = rows.clone().map(move |row| {
            match &mut probe {
                Some(probe) => probe.next().flatten(),
                None => self.cells.get(row),
            }.expect("A fragment has this row")
        });
        if let Some((first, _)) = cells.clone().next()
            && cells.clone().any(|(source, _)| layout(source) != layout(first)) {
            return Ok(None);
        }
        let first = cells.clone().next().and_then(|(source, _)| self.sources[source].as_ref());
        let Some((_, field, logical, _)) = first else {
            let name = self.sources.iter().flatten().next().map_or("", |(_, field, ..)| field.name());
            let array = Arc::new(arrow_array::NullArray::new(rows.len())) as ArrayRef;
            let field = Arc::new(ArrowField::new(name, DataType::Null, true));
            let reservation = memory.reserve(array.get_array_memory_size())?;
            return Ok(Some(((array, field, Some(LogicalType::Null)), reservation)));
        };

        let sources = self.sources.len().min(cells.len());
        let base = sources.checked_mul(64)
            .and_then(|bytes| field.size().checked_mul(8).and_then(|schema| bytes.checked_add(schema)))
            .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
        let bound = cells.clone().try_fold(base, |bytes, (source, row)| {
            let (array, field, ..) = self.sources[source].as_ref().ok_or_else(|| invalid("fragment-layout", "Fragment rows differ in layout"))?;
            rtdb_columnar::estimate_copy_cell_bytes(field, array.as_ref(), row)?.checked_mul(4)
                .and_then(|size| bytes.checked_add(size)).ok_or_else(size_error)
        })?;
        let mut reservation = memory.reserve(bound)?;
        let mut inputs = Vec::with_capacity(sources);
        let mut positions = HashMap::with_capacity(sources);
        let mut picks = Vec::with_capacity(cells.len());
        for (source, row) in cells {
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
        Ok(Some(((output, Arc::clone(field), logical.clone()), reservation)))
    }

    fn null_test(&self, is_null: bool) -> Option<Vec<Option<bool>>> {
        if !self.sources.iter().flatten().all(|(_, field, logical, _)| logical.as_ref().is_some_and(|ty| stable_read_layout(ty, field))) {
            return None;
        }
        self.arrow_cells().map(|cell| {
            let null = match cell {
                Some((array, field, _, row)) => arrow_cell_is_null(field, array, row)?,
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
    pub(crate) fn borrow_plain_arrow(array: &ArrayRef, field: &Arc<ArrowField>) -> Self {
        let logical = match field.data_type() {
            DataType::Boolean | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            | DataType::Float32 | DataType::Float64 | DataType::Utf8 | DataType::Binary
            | DataType::Date32 | DataType::Timestamp(_, _) | DataType::Null if field.metadata().is_empty() => {
                rtdb_columnar::from_field(field).ok().filter(|ty| *ty != LogicalType::Nothing)
            }
            _ => None,
        };
        Self::Arrow { array: Arc::clone(array), field: Arc::clone(field), logical }
    }

    pub(crate) fn funded_arrow_cell<'a>(&'a self, row: usize, owner: &'a Arc<MemoryReservation>)
        -> Option<Option<FundedArrowCell<'a>>> {
        match self {
            Self::Arrow { array, field, logical } => Some(Some(FundedArrowCell { array, field, logical: logical.as_ref(), owner, row })),
            Self::Selected(column) => Some(column.source(row).ok()?.map(|((array, field, logical, custody), row)| {
                FundedArrowCell { array, field, logical: logical.as_ref(), owner: custody.as_ref().unwrap_or(owner), row }
            })),
            Self::Values(_) => None,
        }
    }

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
            admit_cell_read(field, array, logical, offset, memory)?;
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
                let cells = match &column.cells {
                    SelectedRows::Picks(cells) => cells.capacity().checked_mul(std::mem::size_of::<(usize, usize)>()).ok_or_else(size_error)?,
                    SelectedRows::Span(_) | SelectedRows::Expanded { .. } => 0,
                    SelectedRows::Apply(selection) => selection.retained_bytes()?,
                };
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
        if rows == 0 { return Self::empty_with_width(width, memory); }
        let mut output = Self::expansion_batch(rows, width, memory)?;
        for (batch, expansion) in [(left, RowExpansion::RepeatEach(right.len)), (right, RowExpansion::RepeatPage(right.len))] {
            for position in 0..batch.columns.len() {
                context.check_cancelled()?;
                let (column, custody) = match &batch.columns[position] {
                    Column::Values(values) => {
                        let (packed, owner) = pack_evaluated_column(values, memory)?;
                        expanded_column(&packed, &owner, &expansion, rows, memory)?
                    }
                    column => expanded_column(column, batch.column_reservation(position), &expansion, rows, memory)?,
                };
                output.append_column(column, custody, memory)?;
            }
        }
        context.check_cancelled()?;
        Ok(output)
    }

    fn expansion_batch(len: usize, width: usize, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bytes = width.checked_mul(size_of::<Column>() + size_of::<Arc<MemoryReservation>>())
            .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        let reservation = memory.reserve(bytes)?;
        Ok(Self { columns: Vec::with_capacity(width), len, reservation, appended: Vec::with_capacity(width) })
    }

    pub(crate) fn expand_list(&self, position: usize, ordinality: Option<&str>, outer: bool, context: &crate::QueryContext)
        -> Result<Option<Self>> {
        let Some(expansion) = self.list_expansion(position, None, ordinality.is_some(), outer, context)? else {
            return Ok(None);
        };
        let runs = Arc::clone(&expansion.runs);
        if runs.len() == 0 {
            let width = self.columns.len().checked_add(1 + usize::from(ordinality.is_some())).ok_or_else(size_error)?;
            return Self::empty_with_width(width, &context.memory).map(Some);
        }
        let elements = expansion.elements(&runs);
        self.finish_unnest(runs, (elements, expansion.custody), ordinality, outer, context).map(Some)
    }

    fn list_expansion(&self, position: usize, expected: Option<&LogicalType>, narrow_ordinality: bool, outer: bool,
        context: &crate::QueryContext) -> Result<Option<UnnestExpansion>> {
        let skip_empty_variable_lists = expected.is_some_and(|ty| {
            let ty = match ty { LogicalType::Maybe(inner) => inner.as_ref(), ty => ty };
            !matches!(ty, LogicalType::List { kind: ListKind::FixedSizeList(_), .. })
        });
        let memory = &context.memory;
        let mut selection = memory.reserve(4096)?;
        let mut inputs = HashMap::new();
        let mut layouts = HashSet::new();
        let mut sources = vec![None];
        let mut runs = UnnestRows::new(memory)?;
        let list = &self.columns[position];
        let list_owner = self.column_reservation(position);
        for row in 0..self.len {
            context.check_cancelled_at(row)?;
            let vector = match list {
                Column::Arrow { array, field, logical } => Some((array, field, logical.as_ref(), row, list_owner)),
                Column::Selected(selected) => selected.source(row)?.map(|((array, field, logical, custody), offset)| {
                    (array, field, logical.as_ref(), offset, custody.as_ref().unwrap_or(list_owner))
                }),
                Column::Values(_) => return Ok(None),
            };
            let (source, start, count) = match vector {
                None => (0, 0, 0),
                Some((array, field, logical, offset, custody)) => {
                    if skip_empty_variable_lists && array.as_any().downcast_ref::<ListArray>().is_some_and(|list| {
                        let offsets = list.value_offsets();
                        offsets[offset] == offsets[offset + 1]
                    }) {
                        continue;
                    }
                    let layout = (Arc::as_ptr(field), logical.map(std::ptr::from_ref));
                    if !layouts.contains(&layout) {
                        let lease = Arc::get_mut(&mut selection).expect("Unnest owns the selection reservation");
                        let bytes = field.size().checked_mul(8).and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)?;
                        lease.grow_to(lease.bytes().checked_add(bytes).ok_or_else(size_error)?)?;
                        let inferred = if logical.is_none() && expected.is_some() { rtdb_columnar::from_field(field).ok() } else { None };
                        let Some(logical) = logical.or(inferred.as_ref()) else { return Ok(None); };
                        let readable = readable_list_type(logical, field)
                            || (expected.is_some() && readable_mixed_unnest_layout(logical, field, true));
                        if expected.is_some_and(|expected| expected.join(logical) != *expected)
                            || !stable_read_layout(logical, field) || !readable
                        {
                            return Ok(None);
                        }
                        layouts.insert(layout);
                    }
                    let (array, offset) = if matches!(array.data_type(), DataType::Union(_, arrow_schema::UnionMode::Dense)) {
                        let cell = rtdb_columnar::ArrowCell { array: array.as_ref(), field, row: offset }.physical()?;
                        if !matches!(cell.array.data_type(), DataType::List(_)) { continue; }
                        (cell.array, cell.row)
                    } else {
                        (array.as_ref(), offset)
                    };
                    if array.is_null(offset) || array.data_type() == &DataType::Null {
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
                                let list = array.as_any().downcast_ref::<FixedSizeListArray>().ok_or_else(|| {
                                    rtdb_types::Anomaly::fault("rtdb.query/fixed-list-layout", "Fixed-size list field has an incompatible Arrow array")
                                })?;
                                let width = usize::try_from(*width).map_err(|_| {
                                    rtdb_types::Anomaly::fault("rtdb.query/fixed-list-width", "Fixed-size list width is negative")
                                })?;
                                // Arrow slices rebase the child array, and value_offset narrows its index to i32.
                                let start = offset.checked_mul(width)
                                    .filter(|&start| start.checked_add(width).is_some_and(|end| end <= list.values().len()))
                                    .ok_or_else(|| rtdb_types::Anomaly::fault(
                                        "rtdb.query/fixed-list-span", "Fixed-size list child span exceeds its Arrow array",
                                    ))?;
                                (field, list.values(), start, width)
                            }
                            _ => return Ok(None),
                        };
                        if count == 0 {
                            if outer { runs.push(row, None, narrow_ordinality, memory)?; }
                            continue;
                        }
                        let key = (Arc::as_ptr(values).cast::<()>(), Arc::as_ptr(field));
                        let source = if let Some(&source) = inputs.get(&key) {
                            source
                        } else {
                            let lease = Arc::get_mut(&mut selection).expect("Unnest owns the selection reservation");
                            let bytes = field.size().checked_mul(8).and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)?;
                            lease.grow_to(lease.bytes().checked_add(bytes).ok_or_else(size_error)?)?;
                            let source = sources.len();
                            let logical = rtdb_columnar::from_field(field)?;
                            sources.push(Some((Arc::clone(values), Arc::clone(field), Some(logical), Some(Arc::clone(custody)))));
                            inputs.insert(key, source);
                            source
                        };
                        (source, start, count)
                    }
                }
            };
            if count != 0 {
                let end = start.checked_add(count).ok_or_else(size_error)?;
                runs.push(row, Some((source, start..end)), narrow_ordinality, memory)?;
            } else if outer {
                runs.push(row, None, narrow_ordinality, memory)?;
            }
        }
        Ok(Some(UnnestExpansion { runs: Arc::new(runs), sources: Arc::new(sources), custody: selection }))
    }

    pub(crate) fn compatible_unnest_argument(&self, position: usize, expected: &LogicalType) -> bool {
        let admits = |field: &ArrowField, logical: Option<&LogicalType>| {
            let inferred = if logical.is_none() { rtdb_columnar::from_field(field).ok() } else { None };
            logical.or(inferred.as_ref()).is_some_and(|actual| {
                expected.join(actual) == *expected && stable_read_layout(actual, field)
            })
        };
        match &self.columns[position] {
            Column::Arrow { field, logical, .. } => admits(field, logical.as_ref()),
            Column::Selected(column) => column.sources.iter().flatten().all(|(_, field, logical, _)| admits(field, logical.as_ref())),
            Column::Values(_) => false,
        }
    }

    pub(crate) async fn expand_filtered_list(&self, position: usize, schema: &rtdb_plan::Schema, predicate: &rtdb_plan::Expr,
        ordinality: Option<&str>, execution: &crate::context::Execution) -> Result<Option<(Self, FilteredUnnestCounts)>> {
        let context = &execution.context;
        let expected = &schema.fields()[position].data_type;
        let Some(expansion) = self.list_expansion(position, Some(expected), false, false, context)? else {
            return Ok(None);
        };
        let candidates = &expansion.runs;
        let candidate_count = candidates.len();
        let mut counts = FilteredUnnestCounts { candidates: candidate_count as u64, survivors: 0 };
        let mut survivors = UnnestRows::new(&context.memory)?;
        if candidate_count != 0 {
            let compiled = functions::Predicate::bind(predicate, schema);
            for start in (0..candidate_count).step_by(FILTERED_UNNEST_CANDIDATE_ROWS) {
                context.check_cancelled()?;
                let window = if candidate_count <= FILTERED_UNNEST_CANDIDATE_ROWS {
                    Arc::clone(candidates)
                } else {
                    let end = start + (candidate_count - start).min(FILTERED_UNNEST_CANDIDATE_ROWS);
                    candidates.candidate_window(start..end, &context.memory)?
                };
                let batch = self.finish_filtered_unnest(&expansion, &window, ordinality, context)?;
                let selection = admitted_predicate_selection(&batch, predicate, schema, execution, compiled.as_ref(), None).await?;
                counts.survivors += selection.count as u64;
                if selection.count == candidate_count && candidates.groups.len() == self.len {
                    return Ok(Some((batch, counts)));
                }
                survivors.append_survivors(&window, &selection, context)?;
            }
        }
        survivors.fill_unmatched_until(self.len, context)?;
        let output = self.finish_filtered_unnest(&expansion, &Arc::new(survivors), ordinality, context)?;
        Ok(Some((output, counts)))
    }

    fn finish_filtered_unnest(&self, expansion: &UnnestExpansion, runs: &Arc<UnnestRows>, ordinality: Option<&str>,
        context: &crate::QueryContext) -> Result<Self> {
        let memory = &context.memory;
        let mut output = self.expand_unnest_parents(runs, 1 + usize::from(ordinality.is_some()), context)?;
        output.append_funded_column(expansion.elements(runs), Arc::clone(&expansion.custody), memory)?;
        if let Some(name) = ordinality {
            let bytes = runs.len().checked_mul(size_of::<i64>() + 1)
                .and_then(|bytes| bytes.checked_add(name.len())).and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)?;
            let custody = memory.reserve(bytes)?;
            let mut values = arrow_array::builder::Int64Builder::with_capacity(runs.len());
            for (index, group) in runs.groups.iter().enumerate() {
                context.check_cancelled_at(index)?;
                if group.child.is_none() {
                    values.append_null();
                    continue;
                }
                for ordinal in 0..group.end - runs.group_start(index) {
                    context.check_cancelled_at(ordinal)?;
                    let original = i64::try_from(group.ordinal_start + ordinal + 1)
                        .map_err(|_| invalid("ordinality-overflow", "List ordinal exceeds signed64 bits"))?;
                    values.append_value(original);
                }
            }
            let field = Arc::new(ArrowField::new(name, DataType::Int64, true));
            let logical = Some(LogicalType::Scalar(ScalarType::I64).maybe());
            output.append_column(Column::Arrow { array: Arc::new(values.finish()), field, logical }, custody, memory)?;
        }
        Ok(output)
    }

    fn expand_unnest_parents(&self, runs: &Arc<UnnestRows>, dependent_width: usize, context: &crate::QueryContext) -> Result<Self> {
        let memory = &context.memory;
        let rows = runs.len();
        let width = self.columns.len().checked_add(dependent_width).ok_or_else(size_error)?;
        let mut output = Self::expansion_batch(rows, width, memory)?;
        let parents = RowExpansion::UnnestParents(Arc::clone(runs));
        for (position, column) in self.columns.iter().enumerate() {
            context.check_cancelled()?;
            let (column, charge) = match column {
                Column::Values(values) => {
                    let base = runs.groups.len().checked_mul(size_of::<Value>()).ok_or_else(size_error)?;
                    let bytes = runs.groups.iter().try_fold(base, |bytes, group| {
                        bytes.checked_add(values[group.parent].retained_bytes()?).ok_or_else(size_error)
                    })?;
                    let scratch = memory.reserve(bytes)?;
                    let values = runs.groups.iter().map(|group| values[group.parent].clone()).collect::<Vec<_>>();
                    let (packed, owner) = pack_evaluated_column(&values, memory)?;
                    drop((values, scratch));
                    expanded_column(&packed, &owner, &RowExpansion::UnnestGroups(Arc::clone(runs)), rows, memory)?
                }
                column => expanded_column(column, self.column_reservation(position), &parents, rows, memory)?,
            };
            output.append_column(column, charge, memory)?;
        }
        Ok(output)
    }

    fn finish_unnest(&self, runs: Arc<UnnestRows>, (elements, custody): (Column, Arc<MemoryReservation>),
        ordinality: Option<&str>, outer: bool, context: &crate::QueryContext) -> Result<Self> {
        let memory = &context.memory;
        let rows = runs.len();
        let mut output = self.expand_unnest_parents(&runs, 1 + usize::from(ordinality.is_some()), context)?;
        output.append_column(elements, custody, memory)?;
        if let Some(name) = ordinality {
            let bytes = rows.checked_mul(size_of::<i32>() + 1)
                .and_then(|bytes| bytes.checked_add(name.len())).and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)?;
            let charge = memory.reserve(bytes)?;
            let mut builder = Int32Builder::with_capacity(rows);
            for (index, group) in runs.groups.iter().enumerate() {
                context.check_cancelled_at(index)?;
                if group.child.is_none() {
                    builder.append_null();
                    continue;
                }
                let count = group.end - runs.group_start(index);
                for ordinal in 0..count {
                    context.check_cancelled_at(ordinal)?;
                    builder.append_value((ordinal + 1) as i32);
                }
            }
            let array = Arc::new(builder.finish());
            let field = Arc::new(ArrowField::new(name, DataType::Int32, outer));
            let logical = LogicalType::Scalar(ScalarType::I32);
            let column = Column::Arrow { array, field, logical: Some(if outer { logical.maybe() } else { logical }) };
            output.append_column(column, charge, memory)?;
        }
        context.check_cancelled()?;
        Ok(output)
    }

    pub(crate) fn expand_evaluated_lists(&self, runs: UnnestRows, children: &[Self], ordinality: Option<&str>, outer: bool,
        context: &crate::QueryContext) -> Result<Option<Self>> {
        if runs.len() == 0 { return Ok(None); }
        let memory = &context.memory;
        let runs = Arc::new(runs);
        let (elements, custody) = if children.iter().all(|child| matches!(&child.columns[0], Column::Arrow { .. })) {
            let bytes = (children.len() + 1).checked_mul(size_of::<Option<StoredVector>>())
                .and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)?;
            let bytes = children.iter().try_fold(bytes, |bytes, child| {
                let Column::Arrow { logical, .. } = &child.columns[0] else { unreachable!() };
                bytes.checked_add(logical.as_ref().map_or(Ok(0), LogicalType::retained_bytes)?).ok_or_else(size_error)
            })?;
            let custody = memory.reserve(bytes)?;
            let mut sources = Vec::with_capacity(children.len() + 1);
            sources.push(None);
            for (index, child) in children.iter().enumerate() {
                context.check_cancelled_at(index)?;
                let Column::Arrow { array, field, logical } = &child.columns[0] else { unreachable!() };
                sources.push(expansion_source(array, field, logical, child.column_reservation(0)));
            }
            let cells = SelectedRows::Expanded { expansion: RowExpansion::UnnestChildren(Arc::clone(&runs)), rows: 0..runs.len(), input: None };
            (Column::Selected(SelectedColumn { sources: Arc::new(sources), cells }), custody)
        } else {
            let cells = runs.groups.iter().enumerate().flat_map(|(index, group)| {
                let child = group.child.map(|(source, start)| {
                    let child = &children[source - 1];
                    (&child.columns[0], child.column_reservation(0), start)
                });
                (0..group.end - runs.group_start(index))
                    .map(move |offset| child.map(|(column, owner, start)| (column, owner, start + offset)))
            });
            take_arrow_rows(cells, runs.len(), true, memory, Some(context))?
        };
        self.finish_unnest(runs, (elements, custody), ordinality, outer, context).map(Some)
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
        self.append_funded_column(column, reservation, memory)
    }

    pub(crate) fn append_funded_column(&mut self, column: Column, reservation: Arc<MemoryReservation>, memory: &Arc<MemoryBudget>) -> Result<()> {
        let old_columns = self.columns.len().checked_mul(size_of::<Column>()).ok_or_else(size_error)?;
        let old_leases = self.appended.len().checked_mul(size_of::<Arc<MemoryReservation>>()).ok_or_else(size_error)?;
        let _scratch = memory.reserve(old_columns.checked_add(old_leases).ok_or_else(size_error)?)?;
        self.columns.reserve_exact(1);
        self.appended.reserve_exact(1);
        self.columns.push(column);
        self.appended.push(reservation);
        Ok(())
    }

    pub(crate) fn column_reservation(&self, position: usize) -> &Arc<MemoryReservation> {
        let offset = self.columns.len() - self.appended.len();
        position.checked_sub(offset).map_or(&self.reservation, |position| &self.appended[position])
    }

    pub(crate) fn lend_window_span(&self, position: usize, rows: Range<usize>, memory: &Arc<MemoryBudget>)
        -> Result<Option<(Column, Arc<MemoryReservation>)>> {
        let Some(((array, field, logical, owner), rows)) = self.window_span_source(position, rows) else {
            return Ok(None);
        };
        let bytes = logical.as_ref().map_or(Ok(0), LogicalType::retained_bytes)?
            .checked_add(512).ok_or_else(size_error)?;
        let reservation = memory.reserve(bytes)?;
        let sources = Arc::new(vec![Some((Arc::clone(array), Arc::clone(field), logical.clone(), Some(Arc::clone(owner))))]);
        Ok(Some((Column::Selected(SelectedColumn { sources, cells: SelectedRows::Span(rows) }), reservation)))
    }

    pub(crate) fn sharing(columns: Vec<Column>, len: usize, reservation: Arc<MemoryReservation>) -> Self {
        Self { columns, len, reservation, appended: Vec::new() }
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
        let width = self.columns.len();
        let base = width.checked_mul(size_of::<Column>() + size_of::<Arc<MemoryReservation>>())
            .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        let bytes = self.columns.iter().try_fold(base, |bytes, column| {
            let copied = match column {
                Column::Arrow { logical, .. } => logical.as_ref().map_or(Ok(0), |logical| {
                    logical.retained_bytes()?.checked_sub(size_of::<LogicalType>()).ok_or_else(size_error)
                })?,
                Column::Selected(_) => 0,
                Column::Values(values) => values.iter().try_fold(0usize, |bytes, value| {
                    let shared = Self::replay_value_backing(value)?;
                    let owned = value.retained_bytes()?.checked_sub(shared).ok_or_else(size_error)?;
                    bytes.checked_add(owned).ok_or_else(size_error)
                })?,
            };
            bytes.checked_add(copied).ok_or_else(size_error)
        })?;
        let reservation = memory.reserve(bytes)?;
        let mut columns = Vec::with_capacity(width);
        let mut appended = Vec::with_capacity(width);
        for (position, column) in self.columns.iter().enumerate() {
            let shares_storage = match column {
                Column::Arrow { .. } | Column::Selected(_) => true,
                Column::Values(values) => values.iter().try_fold(false, |shared, value| {
                    Ok::<_, rtdb_types::Anomaly>(shared || Self::replay_value_backing(value)? != 0)
                })?,
            };
            let owner = if shares_storage { self.column_reservation(position) } else { &reservation };
            columns.push(column.duplicate());
            appended.push(Arc::clone(owner));
        }
        Ok(Self { columns, len: self.len, reservation, appended })
    }

    fn replay_value_backing(value: &Value) -> Result<usize> {
        fn sum<'a>(mut values: impl Iterator<Item = &'a Value>) -> Result<usize> {
            values.try_fold(0usize, |bytes, value| {
                bytes.checked_add(ColumnBatch::replay_value_backing(value)?).ok_or_else(size_error)
            })
        }

        match value {
            Value::List(list) | Value::FixedSizeList(list) => match list.view() {
                rtdb_types::ListView::Values(values) => sum(values.iter()),
                _ => list.retained_payload_bytes(),
            },
            Value::Set(values) => sum(values.iter()),
            Value::Map { entries, .. } => sum(entries.iter().flat_map(|(key, value)| [key, value])),
            Value::Struct(fields) => sum(fields.values()),
            _ => Ok(0),
        }
    }

    fn page_charges(&self) -> impl Iterator<Item = &Arc<MemoryReservation>> {
        std::iter::once(&self.reservation).chain(&self.appended)
    }

    pub(crate) fn validate_readable(&self, context: &crate::QueryContext) -> Result<()> {
        if self.len == 0 { return Ok(()); }
        context.check_cancelled_at(0)?;
        let mut checks = Vec::<BatchReadCheck<'_>>::new();
        let mut scratch = None;
        for column in &self.columns {
            let check = match column {
                Column::Arrow { array, field, logical } => {
                    let check = admit_cell_read(field, array.as_ref(), logical.as_ref(), 0, &context.memory)?;
                    if array.len() >= self.len && !check.requires_cell_validation() { continue; }
                    BatchReadCheck::Arrow { array: array.as_ref(), check }
                }
                Column::Selected(column) => {
                    let bytes = column.sources.len().checked_mul(size_of::<Option<CellReadCheck<'_>>>()).ok_or_else(size_error)?;
                    let reservation = context.memory.reserve(bytes)?;
                    let mut check = BatchReadCheck::Selected {
                        column, probe: column.cells.join_probe_rows(0..column.cells.len()),
                        checks: std::iter::repeat_with(|| None).take(column.sources.len()).collect(), _reservation: reservation,
                    };
                    check.validate(0, &context.memory)?;
                    check
                }
                Column::Values(_) => continue,
            };
            if scratch.is_none() {
                let bytes = self.columns.len().checked_mul(size_of::<BatchReadCheck<'_>>()).ok_or_else(size_error)?;
                scratch = Some(context.memory.reserve(bytes)?);
                checks.reserve_exact(self.columns.len());
            }
            checks.push(check);
        }
        if checks.is_empty() { return Ok(()); }
        for row in 1..self.len {
            context.check_cancelled_at(row)?;
            for check in &mut checks { check.validate(row, &context.memory)?; }
        }
        Ok(())
    }

    pub(crate) fn working_set_bytes(&self, memory: &Arc<MemoryBudget>) -> Result<usize> {
        let selections = self.columns.iter().filter_map(|column| match column {
            Column::Selected(selected) => Some(selected),
            _ => None,
        });
        let sources = selections.flat_map(|selected| {
            let vector_owners = selected.sources.iter().flatten().filter_map(|(_, _, _, owner)| owner.as_ref());
            vector_owners.chain(selected.cells.expansion_owners())
        });
        distinct_charges(self.page_charges().chain(sources), memory)
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

    pub(crate) fn retain_ordered_list_span(&self, rows: Range<usize>, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        let owner = self.column_reservation(0);
        let (array, field, logical, owner, rows) = match &self.columns[0] {
            Column::Arrow { array, field, logical } => (array, field, logical, owner, rows),
            Column::Selected(selected) => {
                let Some(((array, field, logical, custody), span)) = selected.contiguous_source(rows) else { return Ok(None); };
                (array, field, logical, custody.as_ref().unwrap_or(owner), span)
            }
            Column::Values(_) => return Ok(None),
        };
        if rows.end > array.len() || array.data_type() != field.data_type() || !can_interleave_layout(array.as_ref(), true)
            || array.data_type() == &DataType::Null
        {
            return Ok(None);
        }
        let bytes = field.size().checked_mul(8)
            .and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)?;
        let bytes = bytes.checked_add(logical.as_ref().map_or(Ok(0), LogicalType::retained_bytes)?).ok_or_else(size_error)?;
        let reservation = memory.reserve_with_backing(bytes, std::iter::once(Arc::clone(owner)))?;
        let view = if rows.len() == array.len() { Arc::clone(array) } else { array.slice(rows.start, rows.len()) };
        // A slice preserves the payload of null slots that list compaction clears.
        if scalar_layout(view.data_type()) && view.null_count() != 0 { return Ok(None); }
        let visible = rtdb_columnar::visible_buffer_bytes(std::slice::from_ref(&view)).unwrap_or(0);
        let backing = distinct_charges(std::iter::once(owner), memory)?.max(view.get_buffer_memory_size());
        if backing > visible.saturating_mul(2).saturating_add(4096) { return Ok(None); }
        let column = Column::Arrow { array: view, field: Arc::clone(field), logical: logical.clone() };
        Ok(Some(Self::sharing(vec![column], rows.len(), reservation)))
    }

    pub(crate) fn collect_lists_from_rows(
        sources: &[Self], rows: impl Iterator<Item = (usize, usize)> + Clone, len: usize,
        offsets: Vec<i32>, valid: Vec<bool>, output: &rtdb_plan::Field, memory: &Arc<MemoryBudget>,
    ) -> Result<(Column, Arc<MemoryReservation>)> {
        if len == 0 {
            return Self::sharing(vec![], 0, memory.reserve(256)?).collect_lists(offsets, valid, output, memory);
        }
        if let [source] = sources && source.len == len
            && !source.column_reservation(0).backing().is_empty()
            && rows.clone().eq((0..len).map(|row| (0, row)))
            && let [Column::Arrow { array, field, logical }] = source.columns.as_slice()
            && scalar_layout(array.data_type()) && array.data_type() != &DataType::Null && array.null_count() == 0
        {
            let physical = rtdb_columnar::to_field(&output.name, &output.data_type)?;
            let DataType::List(item) = physical.data_type() else { return Err(invalid("column-list", "Expected a list result field")); };
            let bytes = valid.len().checked_mul(32)
                .and_then(|bytes| field.size().checked_mul(8).and_then(|size| bytes.checked_add(size)))
                .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
            let owner = Arc::clone(source.column_reservation(0));
            let reservation = memory.reserve_with_backing(bytes, std::iter::once(owner))?;
            let item = if logical.as_ref().is_some_and(|ty| stable_read_layout(ty, field)) {
                Arc::new(field.as_ref().clone().with_name(item.name()))
            } else {
                Arc::clone(field)
            };
            let column = Self::finish_list(&physical, offsets, valid, Arc::clone(array), item)?;
            return Ok((column, reservation));
        }
        let (column, charge) = gather_column_cells(|page| &sources[page], rows.map(Some), len, 0, false, memory)?;
        Self::settled(vec![column], len, charge)?.collect_lists(offsets, valid, output, memory)
    }

    pub(crate) fn collect_lists(
        &self, offsets: Vec<i32>, valid: Vec<bool>, field: &rtdb_plan::Field, memory: &Arc<MemoryBudget>,
    ) -> Result<(Column, Arc<MemoryReservation>)> {
        let physical = rtdb_columnar::to_field(&field.name, &field.data_type)?;
        let DataType::List(item) = physical.data_type() else { return Err(invalid("column-list", "Expected a list result field")); };
        let mut child_field = field.clone();
        child_field.name = item.name().clone();
        child_field.data_type = rtdb_columnar::from_field(item)?;
        let schema = rtdb_plan::Schema::new(vec![child_field])?;
        let record = if self.len == 0 { None } else { match self.physical_record(&schema, memory) {
            Ok(record) => record,
            Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => None,
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
        let column = Self::finish_list(&physical, offsets, valid, child, item)?;
        Ok((column, reservation))
    }

    fn finish_list(physical: &ArrowField, offsets: Vec<i32>, valid: Vec<bool>, child: ArrayRef, item: Arc<ArrowField>) -> Result<Column> {
        use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
        let array = ListArray::try_new(item, OffsetBuffer::new(ScalarBuffer::from(offsets)), child, Some(NullBuffer::from(valid)))
            .map_err(|error| invalid("column-list", error))?;
        let field = Arc::new(ArrowField::new(physical.name(), array.data_type().clone(), physical.is_nullable() || array.null_count() != 0));
        let logical = rtdb_columnar::from_field(&field)?;
        Ok(Column::Arrow { array: Arc::new(array), field, logical: Some(logical) })
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
                Column::Selected(selected) => match selected.uniform_fragment_vector(0..self.len, memory)? {
                    Some((vector, scratch)) => (vector, Some(scratch)),
                    None => return Ok(None),
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

    pub(crate) fn from_packed_list(
        array: ArrayRef, field: Arc<ArrowField>, mut reservation: Arc<MemoryReservation>,
        backing: Option<Arc<MemoryReservation>>,
    ) -> Result<Self> {
        let shared_bytes = if backing.is_some() { array.get_buffer_memory_size() } else { 0 };
        let base = array.get_array_memory_size().checked_sub(shared_bytes)
            .and_then(|bytes| bytes.checked_add(size_of::<Column>() + 256)).ok_or_else(size_error)?;
        let occupied = |field: &ArrowField| base.checked_add(field.size()).ok_or_else(size_error);
        let lease = Arc::get_mut(&mut reservation).expect("A packed list page owns its reservation");
        let field = if field.name() == "value" {
            field
        } else {
            let bound = occupied(&field)?
                .checked_add(field.size()).and_then(|bytes| bytes.checked_add(64)).ok_or_else(size_error)?;
            if bound > lease.bytes() { lease.grow_to(bound)?; }
            Arc::new(Arc::unwrap_or_clone(field).with_name("value"))
        };
        let retained = occupied(&field)?;
        if retained > lease.bytes() { lease.grow_to(retained)?; } else { lease.shrink_to(retained)?; }
        let reservation = match backing {
            Some(backing) => reservation.budget().reserve_with_backing(0, [Arc::clone(&reservation), backing].into_iter())?,
            None => reservation,
        };
        let len = array.len();
        Ok(Self::sharing(vec![Column::Arrow { array, field, logical: None }], len, reservation))
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

    pub(crate) fn take_sorted_span(&self, rows: Range<usize>, positions: &[usize], memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        use std::mem::size_of;

        if rows.is_empty() || rows.end > self.len { return Ok(None); }
        let mut metadata = positions.len().checked_mul(size_of::<Column>() + size_of::<Arc<MemoryReservation>>())
            .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        for &position in positions {
            let Some(Column::Arrow { array, field, logical }) = self.columns.get(position) else { return Ok(None); };
            if rows.end > array.len() || array.data_type() != field.data_type()
                || !scalar_layout(array.data_type()) || array.data_type() == &DataType::Null
            {
                return Ok(None);
            }
            if array.null_count() != 0 && matches!(array.data_type(), DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary) {
                for row in rows.clone() {
                    if array.is_null(row) && flat_copy_bytes(field, array.as_ref(), row)? != 0 { return Ok(None); }
                }
            }
            let descriptor = array.get_array_memory_size().saturating_sub(array.get_buffer_memory_size());
            let logical = logical.as_ref().map_or(Ok(0), LogicalType::retained_bytes)?;
            metadata = metadata.checked_add(descriptor).and_then(|bytes| bytes.checked_add(logical)).ok_or_else(size_error)?;
        }
        let reservation = memory.reserve(metadata)?;
        let scratch = positions.len().checked_mul(256).ok_or_else(size_error)?;
        let _scratch = memory.reserve(scratch)?;
        let mut columns = Vec::with_capacity(positions.len());
        let mut appended = Vec::with_capacity(positions.len());
        let mut views = Vec::with_capacity(positions.len());
        for &position in positions {
            let Column::Arrow { array, .. } = &self.columns[position] else { unreachable!("The sort projection was admitted") };
            let view = if rows.len() == array.len() { Arc::clone(array) } else { array.slice(rows.start, rows.len()) };
            views.push(view);
            appended.push(Arc::clone(self.column_reservation(position)));
        }
        let visible = rtdb_columnar::visible_buffer_bytes(&views).unwrap_or(0);
        // A row can be small even when its view pins a large variable-width or shared page allocation.
        let backing = distinct_charges(&appended, memory)?
            .max(views.iter().map(|array| array.get_buffer_memory_size()).max().unwrap_or(0));
        let compact = backing > visible.saturating_mul(2).saturating_add(4096);
        for (index, (&position, view)) in positions.iter().zip(views).enumerate() {
            let Column::Arrow { array, field, logical } = &self.columns[position] else { unreachable!("The sort projection was admitted") };
            let array = if compact {
                let (array, owner) = compact_arrow_rows(array.as_ref(), field, rows.clone(), memory)?;
                appended[index] = owner;
                array
            } else {
                view
            };
            columns.push(Column::Arrow { array, field: Arc::clone(field), logical: logical.clone() });
        }
        Ok(Some(Self { columns, len: rows.len(), reservation, appended }))
    }

    pub(crate) fn select_keys(&self, rows: &[(usize, usize)], memory: &Arc<MemoryBudget>) -> Result<Self> {
        if let Some(&(0, start)) = rows.first()
            && let Some(end) = start.checked_add(rows.len()).filter(|&end| end <= self.len)
            && rows.iter().copied().eq((start..end).map(|row| (0, row)))
        {
            match self.select_key_span(start..end, memory) {
                Ok(Some(keys)) => return Ok(keys),
                Ok(None) => {}
                Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => {}
                Err(error) => return Err(error),
            }
        }
        let positions: Vec<_> = (0..self.columns.len()).collect();
        if rows.len() < self.len {
            match Self::take_rows_with_custody(&[self], rows, &positions, false, memory) {
                Ok(output) => return Ok(output),
                Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => {}
                Err(error) => return Err(error),
            }
        }
        Self::take_rows_with_custody(&[self], rows, &positions, true, memory)
    }

    fn select_key_span(&self, rows: Range<usize>, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        let width = self.columns.len();
        let base = width.checked_mul(size_of::<Column>() + size_of::<Arc<MemoryReservation>>())
            .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        let scratch = width.checked_mul(256).and_then(|bytes| bytes.checked_add(base)).ok_or_else(size_error)?;
        let mut reservation = memory.reserve(scratch)?;
        let lease = Arc::get_mut(&mut reservation).expect("The key span owns its metadata");
        let mut metadata = base;
        let mut descriptors = 0usize;
        let mut columns = Vec::with_capacity(width);
        let mut appended = Vec::with_capacity(width);
        let mut views = Vec::with_capacity(width);
        for (position, column) in self.columns.iter().enumerate() {
            let owner = self.column_reservation(position);
            let (array, field, logical, owner, span) = match column {
                Column::Arrow { array, field, logical } => (array, field, logical, owner, rows.clone()),
                Column::Selected(selection) => {
                    let Some(((array, field, logical, custody), span)) = selection.contiguous_source(rows.clone()) else {
                        return Ok(None);
                    };
                    (array, field, logical, custody.as_ref().unwrap_or(owner), span)
                }
                Column::Values(_) => return Ok(None),
            };
            if span.end > array.len() || array.data_type() != field.data_type() {
                return Ok(None);
            }
            let descriptor = array.get_array_memory_size().saturating_sub(array.get_buffer_memory_size());
            let whole = span.len() == array.len();
            let logical_bytes = logical.as_ref().map_or(Ok(0), LogicalType::retained_bytes)?;
            metadata = metadata.checked_add(logical_bytes).ok_or_else(size_error)?;
            descriptors = descriptors.checked_add(if whole { 0 } else { descriptor }).ok_or_else(size_error)?;
            let scratch = descriptor.checked_mul(4).and_then(|bytes| bytes.checked_add(logical_bytes)).ok_or_else(size_error)?;
            lease.grow_to(lease.bytes().checked_add(scratch).ok_or_else(size_error)?)?;
            let view = if whole { Arc::clone(array) } else { array.slice(span.start, span.len()) };
            views.push(Arc::clone(&view));
            appended.push(Arc::clone(owner));
            columns.push(Column::Arrow { array: view, field: Arc::clone(field), logical: logical.clone() });
        }
        let visible = rtdb_columnar::visible_buffer_bytes(&views).unwrap_or(0);
        let backing = distinct_charges(&appended, memory)?
            .max(views.iter().map(|array| array.get_buffer_memory_size()).max().unwrap_or(0));
        drop(views);
        if backing > visible.saturating_mul(2).saturating_add(4096) {
            for (column, owner) in columns.iter_mut().zip(&mut appended) {
                let Column::Arrow { array, field, .. } = column else { unreachable!("Key span columns have Arrow layouts") };
                if !can_interleave_layout(array.as_ref(), true) { return Ok(None); }
                let Ok((compact, custody)) = compact_arrow_rows(array.as_ref(), field, 0..rows.len(), memory) else {
                    return Ok(None);
                };
                *array = compact;
                *owner = custody;
            }
            descriptors = 0;
        }
        lease.shrink_to(metadata.checked_add(descriptors).ok_or_else(size_error)?)?;
        Ok(Some(Self { columns, len: rows.len(), reservation, appended }))
    }

    pub(crate) fn key_projection(&self, positions: &[usize], memory: &Arc<MemoryBudget>) -> Result<Self> {
        self.project_keys(positions, false, memory)
    }

    pub(crate) fn membership_build_key(
        &self, position: usize, memory: &Arc<MemoryBudget>,
    ) -> Result<Self> {
        let owner = self.column_reservation(position);
        let (array, field, logical, owner, rows) = match &self.columns[position] {
            Column::Arrow { array, field, logical } => (array, field, logical, owner, 0..self.len),
            Column::Selected(selection) => {
                let Some(((array, field, logical, custody), rows)) =
                    selection.contiguous_source(0..self.len)
                else {
                    return self.project_keys(&[position], true, memory);
                };
                (array, field, logical, custody.as_ref().unwrap_or(owner), rows)
            }
            Column::Values(values) => {
                let (column, reservation) = pack_evaluated_column(values, memory)?;
                return Self::settled(vec![column], self.len, reservation);
            }
        };
        let logical_bytes = logical.as_ref().map_or(Ok(0), LogicalType::retained_bytes)?;
        let metadata = field.size().checked_mul(8)
            .and_then(|bytes| bytes.checked_add(logical_bytes))
            .and_then(|bytes| bytes.checked_add(512))
            .ok_or_else(size_error)?;
        let reservation = memory.reserve(metadata)?;
        let view = if rows == (0..array.len()) {
            Arc::clone(array)
        } else {
            array.slice(rows.start, rows.len())
        };
        let visible = rtdb_columnar::visible_buffer_bytes(std::slice::from_ref(&view)).unwrap_or(0);
        let backing = distinct_charges(std::iter::once(owner), memory)?.max(array.get_buffer_memory_size());
        let (array, custody) = if backing <= visible.saturating_mul(2).saturating_add(4096) {
            (view, Arc::clone(owner))
        } else {
            compact_arrow_rows(array.as_ref(), field, rows, memory)?
        };
        let column = Column::Arrow { array, field: Arc::clone(field), logical: logical.clone() };
        Ok(Self { columns: vec![column], len: self.len, reservation, appended: vec![custody] })
    }

    pub(crate) fn compact_ranked_projection(&self, row: usize, positions: &[usize], memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        const MAX_ROW_BYTES: usize = 64 * 1024;
        let base = positions.len().saturating_mul(std::mem::size_of::<Column>()).saturating_add(256);
        if base > MAX_ROW_BYTES { return Ok(None); }
        let mut reservation = memory.reserve(base)?;
        let lease = Arc::get_mut(&mut reservation).expect("A ranked projection owns its reservation");
        let mut columns = Vec::with_capacity(positions.len());
        for &position in positions {
            let Some(Some((array, field, logical, offset))) = self.columns[position].arrow_cell(row) else { return Ok(None); };
            if !flat(array.data_type()) { return Ok(None); }
            let retained = lease.bytes();
            let schema = field.size().saturating_add(logical.map_or(Ok(0), LogicalType::retained_bytes)?);
            let bound = rtdb_columnar::estimate_copy_cell_bytes(field, array, offset)?.saturating_mul(4)
                .saturating_add(schema.saturating_mul(8)).saturating_add(4096);
            if retained.saturating_add(bound) > MAX_ROW_BYTES { return Ok(None); }
            lease.grow_to(retained + bound)?;
            // A contiguous slice could keep the IPC allocation for the entire primary page alive.
            let array = copy_selected_cells(&[array], &[(0, offset)])?;
            let bytes = retained.saturating_add(schema).saturating_add(array.get_array_memory_size());
            lease.shrink_to(bytes)?;
            columns.push(Column::Arrow { array, field: Arc::new(field.clone()), logical: logical.cloned() });
        }
        Ok(Some(Self::sharing(columns, 1, reservation)))
    }

    #[cfg(test)]
    pub(crate) fn compact_group_keys(&self, rows: &[(usize, usize)], memory: &Arc<MemoryBudget>) -> Result<Self> {
        self.compact_group_key_columns(rows, None, memory)
    }

    pub(crate) fn compact_group_key_columns(&self, rows: &[(usize, usize)], positions: Option<&[usize]>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let width = positions.map_or(self.columns.len(), <[usize]>::len);
        let _positions = memory.reserve(width.checked_mul(std::mem::size_of::<usize>()).ok_or_else(size_error)?)?;
        let output_positions: Vec<_> = (0..width).collect();
        let picked = Self::take_rows_with_custody(&[self], rows, positions.unwrap_or(&output_positions), true, memory)?;
        picked.project_keys(&output_positions, true, memory)
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
                let first = selection.cells.get(0).and_then(|(source, _)| layout(source));
                if selection.cells.iter().all(|(source, _)| layout(source) == first) {
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

    pub(crate) fn from_spill_fragments(mut sources: Vec<Self>, width: usize, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let outside_source = || invalid("column-gather", "Row position is outside its source batch");
        if sources.is_empty() { return Err(invalid("invalid-spill", "Spilled page has no Arrow fragments")); }
        if sources.iter().any(|source| source.columns.len() < width) { return Err(outside_source()); }
        if sources.len() == 1 { return Ok(sources.pop().expect("One spill fragment remains")); }

        let rows = sources.iter().try_fold(0usize, |rows, source| {
            rows.checked_add(source.len).ok_or_else(|| invalid("invalid-spill", "Spilled page row count overflow"))
        })?;
        if rows == 0 { return Err(outside_source()); }
        let nonempty = sources.iter().filter(|source| source.len != 0);
        let count = nonempty.clone().count();
        let mut expansion = None;
        let bytes = width.checked_mul(size_of::<Column>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        let mut reservation = memory.reserve(bytes)?;
        let lease = Arc::get_mut(&mut reservation).expect("The spill page owns its metadata");
        let mut columns = Vec::with_capacity(width);
        for position in 0..width {
            let vectors = nonempty.clone().map(|source| source.spill_fragment_vector(position));
            let mut first_layout = None;
            let mut keep_spans = false;
            let mut compatible = true;
            let mut bytes = count.checked_mul(size_of::<Option<StoredVector>>())
                .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
            for (source, vector) in nonempty.clone().zip(vectors.clone()) {
                let Some(vector) = vector else {
                    compatible = false;
                    break;
                };
                let layout = vector.map(|(_, field, logical, _)| (field, logical));
                keep_spans |= *first_layout.get_or_insert(layout) != layout
                    || matches!(&source.columns[position], Column::Selected(_));
                if let Some((array, _, logical, _)) = vector {
                    keep_spans |= nested_layout(array.data_type()) || array.data_type() == &DataType::Null;
                    bytes = bytes.checked_add(logical.as_ref().map_or(Ok(0), LogicalType::retained_bytes)?)
                        .ok_or_else(size_error)?;
                }
            }
            if compatible && keep_spans {
                if expansion.is_none() {
                    let span_bytes = count.checked_mul(size_of::<usize>())
                        .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
                    let reservation = memory.reserve(span_bytes)?;
                    let mut end = 0;
                    let ends = nonempty.clone().map(|source| { end += source.len; end }).collect();
                    expansion = Some(RowExpansion::PageSpans(Arc::new(PageSpans { ends, reservation })));
                }
                lease.grow_to(lease.bytes().checked_add(bytes).ok_or_else(size_error)?)?;
                let mut vectors_out = Vec::with_capacity(count);
                vectors_out.extend(vectors.map(|vector| {
                    vector.expect("The spill fragment has a full vector").map(|(array, field, logical, owner)| {
                        (Arc::clone(array), Arc::clone(field), logical.clone(), Some(Arc::clone(owner)))
                    })
                }));
                let cells = SelectedRows::Expanded {
                    expansion: expansion.as_ref().expect("The spill page owns its spans").clone(),
                    rows: 0..rows,
                    input: None,
                };
                columns.push(Column::Selected(SelectedColumn { sources: Arc::new(vectors_out), cells }));
            } else {
                let cells = sources.iter().enumerate().flat_map(|(index, source)| {
                    (0..source.len).map(move |row| Some((index, row)))
                });
                let (column, _scratch) = gather_column_cells(|index| &sources[index], cells, rows, position, false, memory)?;
                lease.grow_to(lease.bytes().checked_add(column.retained_bytes()?).ok_or_else(size_error)?)?;
                columns.push(column);
            }
        }
        Self::settled(columns, rows, reservation)
    }

    fn spill_fragment_vector(&self, position: usize) -> Option<Option<FragmentVector<'_>>> {
        let owner = self.column_reservation(position);
        match &self.columns[position] {
            Column::Arrow { array, field, logical } if array.len() == self.len => {
                let absent = array.data_type() == &DataType::Null && field.is_nullable() && logical.as_ref() != Some(&LogicalType::Nothing);
                Some((!absent).then_some((array, field, logical, owner)))
            }
            Column::Selected(selected) if selected.cells.len() == self.len => {
                let (source, rows) = selected.cells.contiguous(0..self.len)?;
                if rows.start != 0 { return None; }
                match selected.sources.get(source)?.as_ref() {
                    Some((array, field, logical, custody)) if array.len() == self.len => {
                        Some(Some((array, field, logical, custody.as_ref().unwrap_or(owner))))
                    }
                    Some(_) => None,
                    None => Some(None),
                }
            }
            _ => None,
        }
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
    #[cfg(test)]
    pub fn rows(&self, memory: &Arc<MemoryBudget>) -> Result<OwnedPage> {
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
                values.push(column.value(row)?);
            }
            page.push(OwnedRow::with_reservation(values, Arc::clone(&reservation))?)?;
        }
        Ok(page.finish())
    }
}

enum BatchReadCheck<'a> {
    Arrow { array: &'a dyn Array, check: CellReadCheck<'a> },
    Selected {
        column: &'a SelectedColumn,
        probe: Option<JoinProbeRows<'a>>,
        checks: Vec<Option<CellReadCheck<'a>>>,
        _reservation: Arc<MemoryReservation>,
    },
}

impl BatchReadCheck<'_> {
    fn validate(&mut self, row: usize, memory: &Arc<MemoryBudget>) -> Result<()> {
        match self {
            Self::Arrow { array, check } => check.validate(*array, row),
            Self::Selected { column, probe, checks, .. } => {
                let column = *column;
                let cell = match probe {
                    Some(probe) => probe.next().flatten(),
                    None => column.cells.get(row),
                };
                let Some((source, offset)) = cell else { return Ok(()); };
                let Some((array, field, logical, _)) = &column.sources[source] else { return Ok(()); };
                if let Some(check) = &checks[source] {
                    check.validate(array.as_ref(), offset)
                } else {
                    checks[source] = Some(admit_cell_read(field, array.as_ref(), logical.as_ref(), offset, memory)?);
                    Ok(())
                }
            }
        }
    }
}

#[test]
fn batch_validation_checks_later_cells_and_only_the_selected_source_positions() {
    let memory = MemoryBudget::new(1 << 20);
    let database = crate::DatabaseSnapshot {
        partitions: Vec::new(),
        storage: Arc::new(rtdb_storage::cache::BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
    };
    let catalog = crate::CatalogSnapshot::new("xtdb", [("xtdb".into(), database)].into(), false).unwrap();
    let context = crate::QueryContext::new(catalog, Arc::clone(&memory), Timestamp(0), "UTC", Default::default(),
        &tokio_util::sync::CancellationToken::new()).unwrap();

    let logical = LogicalType::Scalar(ScalarType::Uri);
    let field = Arc::new(rtdb_columnar::to_field("uri", &logical).unwrap());
    let array: ArrayRef = Arc::new(arrow_array::StringArray::from(vec!["https://xtdb.com", "not a URI"]));
    let arrow = ColumnBatch::settled(vec![Column::Arrow {
        array: Arc::clone(&array), field: Arc::clone(&field), logical: Some(logical.clone()),
    }], 2, memory.reserve(0).unwrap()).unwrap();
    let expected = arrow.columns[0].validate_readable_cell(1, &memory).unwrap_err();
    assert_eq!(arrow.validate_readable(&context).unwrap_err(), expected);

    let selected = |cells: Vec<(usize, usize)>| {
        let len = cells.len();
        let column = SelectedColumn::new(vec![Some((Arc::clone(&array), Arc::clone(&field), Some(logical.clone()), None)), None], Arc::new(cells));
        ColumnBatch::settled(vec![Column::Selected(column)], len, memory.reserve(0).unwrap()).unwrap()
    };
    selected(vec![(1, 0), (0, 0), (0, 0)]).validate_readable(&context).unwrap();
    assert_eq!(selected(vec![(0, 0), (0, 1)]).validate_readable(&context).unwrap_err(), expected);
}

fn admit_cell_read<'a>(field: &'a ArrowField, array: &dyn Array, logical: Option<&LogicalType>, row: usize,
    memory: &Arc<MemoryBudget>) -> Result<CellReadCheck<'a>> {
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
    CellReadCheck::admit(field, array, logical, row, memory)
}

fn distinct_charges<'a>(leases: impl IntoIterator<Item = &'a Arc<MemoryReservation>>, memory: &Arc<MemoryBudget>) -> Result<usize> {
    fn count(lease: &Arc<MemoryReservation>, seen: &mut HashSet<*const MemoryReservation>, scratch: &mut MemoryReservation) -> Result<usize> {
        let pointer = Arc::as_ptr(lease);
        if seen.contains(&pointer) { return Ok(0); }
        let bytes = seen.len().checked_add(1).and_then(|count| count.checked_mul(64)).ok_or_else(size_error)?;
        scratch.grow_to(bytes)?;
        seen.insert(pointer);
        lease.backing().iter().try_fold(lease.bytes(), |bytes, source| {
            bytes.checked_add(count(source, seen, scratch)?).ok_or_else(size_error)
        })
    }

    let mut leases = leases.into_iter().peekable();
    let Some(first) = leases.next() else { return Ok(0); };
    if first.backing().is_empty() && leases.peek().is_none() { return Ok(first.bytes()); }
    let mut seen = HashSet::new();
    let mut scratch = memory.reserve(0)?;
    let scratch = Arc::get_mut(&mut scratch).expect("Charge inventory owns its reservation");
    let first = count(first, &mut seen, scratch)?;
    leases.try_fold(first, |bytes, lease| bytes.checked_add(count(lease, &mut seen, scratch)?).ok_or_else(size_error))
}

fn expansion_source(array: &ArrayRef, field: &Arc<ArrowField>, logical: &Option<LogicalType>, owner: &Arc<MemoryReservation>) -> Option<StoredVector> {
    let absent = array.data_type() == &DataType::Null && field.is_nullable() && logical.as_ref() != Some(&LogicalType::Nothing);
    (!absent).then(|| (Arc::clone(array), Arc::clone(field), logical.clone(), Some(Arc::clone(owner))))
}

fn expanded_column(column: &Column, owner: &Arc<MemoryReservation>, expansion: &RowExpansion, rows: usize, memory: &Arc<MemoryBudget>)
    -> Result<(Column, Arc<MemoryReservation>)> {
    let (sources, input, charge) = match column {
        Column::Arrow { array, field, logical } => {
            let bytes = logical.as_ref().map_or(Ok(0), LogicalType::retained_bytes)?.checked_add(512).ok_or_else(size_error)?;
            let charge = memory.reserve(bytes)?;
            (Arc::new(vec![expansion_source(array, field, logical, owner)]), None, charge)
        }
        Column::Selected(selected) => {
            let unfunded = selected.sources.iter().flatten().any(|(_, _, _, owner)| owner.is_none());
            let bytes = if unfunded {
                let base = selected.sources.len().checked_mul(size_of::<Option<StoredVector>>())
                    .and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)?;
                selected.sources.iter().flatten().try_fold(base, |bytes, (_, _, logical, _)| {
                    bytes.checked_add(logical.as_ref().map_or(Ok(0), LogicalType::retained_bytes)?).ok_or_else(size_error)
                })?
            } else { 512 };
            let charge = memory.reserve(bytes)?;
            let sources = if unfunded {
                Arc::new(selected.sources.iter().map(|source| source.as_ref().map(|(array, field, logical, custody)| {
                    (Arc::clone(array), Arc::clone(field), logical.clone(), Some(Arc::clone(custody.as_ref().unwrap_or(owner))))
                })).collect())
            } else {
                Arc::clone(&selected.sources)
            };
            let input = Arc::new(ExpansionInput { rows: selected.cells.clone(), owner: Arc::clone(owner) });
            (sources, Some(input), charge)
        }
        Column::Values(_) => unreachable!("Row expansion requires Arrow input"),
    };
    let cells = SelectedRows::Expanded { expansion: expansion.clone(), rows: 0..rows, input };
    Ok((Column::Selected(SelectedColumn { sources, cells }), charge))
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
    gather_column_rows_iter(source, rows.iter().copied(), position, memory)
}

pub(crate) fn gather_column_rows_iter<'a>(
    source: impl Fn(usize) -> &'a ColumnBatch, rows: impl ExactSizeIterator<Item = (usize, usize)> + Clone,
    position: usize, memory: &Arc<MemoryBudget>,
) -> Result<(Column, Arc<MemoryReservation>)> {
    let count = rows.len();
    gather_column_cells(source, rows.map(Some), count, position, false, memory)
}

pub(crate) async fn retain_list_spans(
    pages: &[ColumnBatch], context: &crate::QueryContext,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let [Column::Arrow { field, logical, .. }] = pages[0].columns.as_slice() else { return Ok(None); };
    let memory = &context.memory;
    let span_bytes = pages.len().checked_mul(size_of::<usize>())
        .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
    let logical_bytes = logical.as_ref().map_or(Ok(0), LogicalType::retained_bytes)?;
    let source_bytes = size_of::<Option<StoredVector>>().checked_add(logical_bytes)
        .and_then(|bytes| bytes.checked_mul(pages.len()))
        .and_then(|bytes| bytes.checked_add(1024)).ok_or_else(size_error)?;
    let bound = span_bytes.checked_add(source_bytes).ok_or_else(size_error)?;
    let Some(mut charge) = memory.try_reserve(bound) else { return Ok(None) };
    let reservation = Arc::get_mut(&mut charge).expect("List spans own their metadata").split_off(span_bytes)?;
    let mut ends = Vec::with_capacity(pages.len());
    let mut sources = Vec::with_capacity(pages.len());
    let mut end = 0usize;
    for (index, page) in pages.iter().enumerate() {
        if index % 1024 == 0 {
            tokio::task::yield_now().await;
            context.check_cancelled()?;
        }
        let [Column::Arrow { array, field: other, logical: other_type }] = page.columns.as_slice() else { return Ok(None); };
        if other != field || other_type != logical || array.data_type() != field.data_type() || array.len() != page.len {
            return Ok(None);
        }
        end = end.checked_add(page.len).ok_or_else(size_error)?;
        ends.push(end);
        sources.push(expansion_source(array, other, other_type, page.column_reservation(0)));
    }
    let expansion = RowExpansion::PageSpans(Arc::new(PageSpans { ends, reservation }));
    let cells = SelectedRows::Expanded { expansion, rows: 0..end, input: None };
    Ok(Some((Column::Selected(SelectedColumn { sources: Arc::new(sources), cells }), charge)))
}

pub(crate) fn gather_list_pages(
    pages: &[ColumnBatch], rows: usize, context: &crate::QueryContext,
) -> Result<(Column, Arc<MemoryReservation>)> {
    let positions = pages.iter().flat_map(|page| {
        let column = &page.columns[0];
        let owner = page.column_reservation(0);
        (0..page.len).map(move |row| Some((column, owner, row)))
    });
    take_arrow_rows(positions, rows, false, &context.memory, Some(context))
}

pub(crate) fn gather_nullable_column<'a>(
    source: impl Fn(usize) -> &'a ColumnBatch, rows: &[Option<(usize, usize)>], position: usize, memory: &Arc<MemoryBudget>,
) -> Result<(Column, Arc<MemoryReservation>)> {
    gather_nullable_column_iter(source, rows.iter().copied(), position, memory)
}

pub(crate) fn gather_nullable_column_iter<'a>(
    source: impl Fn(usize) -> &'a ColumnBatch, rows: impl ExactSizeIterator<Item = Option<(usize, usize)>> + Clone,
    position: usize, memory: &Arc<MemoryBudget>,
) -> Result<(Column, Arc<MemoryReservation>)> {
    let count = rows.len();
    let (column, mut reservation) = gather_column_cells(source, rows, count, position, true, memory)?;
    let bytes = column.retained_bytes()?;
    let lease = Arc::get_mut(&mut reservation).expect("A nullable gather owns its reservation");
    if bytes <= lease.bytes() { lease.shrink_to(bytes)?; } else { lease.grow_to(bytes)?; }
    Ok((column, reservation))
}

pub(crate) fn gather_join_probe(
    probe: Option<(&ColumnBatch, crate::batch_join::ProbeRuns)>, count: usize, width: usize,
    columns: &mut Vec<Column>, lease: &mut MemoryReservation,
) -> Result<()> {
    if width == 0 { return Ok(()); }
    let memory = Arc::clone(lease.budget());
    let probe = probe.map(|(batch, runs)| (batch, Arc::new(runs)));
    let expansion = probe.as_ref().map(|(_, runs)| RowExpansion::JoinProbe(Arc::clone(runs)));
    for position in 0..width {
        let (column, mut charge) = match &probe {
            Some((batch, runs)) => match &batch.columns[position] {
                Column::Values(_) => gather_column_cells(|_| *batch, runs.picks(), count, position, true, &memory)?,
                column => {
                    let expansion = expansion.as_ref().expect("A present probe has an expansion");
                    expanded_column(column, batch.column_reservation(position), expansion, count, &memory)?
                }
            },
            None => {
                let charge = memory.reserve(512)?;
                let cells = SelectedRows::Span(0..count);
                let sources = Arc::new(vec![None]);
                (Column::Selected(SelectedColumn { sources, cells }), charge)
            },
        };
        let bytes = column.retained_bytes()?;
        let charge = Arc::get_mut(&mut charge).expect("The probe gather owns its output reservation");
        if bytes <= charge.bytes() { charge.shrink_to(bytes)?; } else { charge.grow_to(bytes)?; }
        lease.absorb(charge)?;
        columns.push(column);
    }
    Ok(())
}

#[test]
fn window_spans_keep_one_source_lease_without_cell_picks() {
    let memory = MemoryBudget::new(1 << 20);
    let mut batch = ColumnBatch::settled(Vec::new(), 4, memory.reserve(256).unwrap()).unwrap();
    let array = Arc::new(arrow_array::Int64Array::from(vec![10, 20, 30, 40]));
    let field = Arc::new(ArrowField::new("shift", DataType::Int64, false));
    let column = Column::Arrow { array, field, logical: Some(LogicalType::Scalar(ScalarType::I64)) };
    batch.append_column(column, memory.reserve(1024).unwrap(), &memory).unwrap();
    let owner = Arc::downgrade(batch.column_reservation(0));
    let (column, charge) = batch.lend_window_span(0, 1..3, &memory).unwrap().unwrap();
    let Column::Selected(selection) = &column else { panic!("Expected a funded span") };
    assert!(matches!(&selection.cells, SelectedRows::Span(rows) if *rows == (1..3)));
    drop(batch);
    assert!(owner.upgrade().is_some());
    assert_eq!(column.value(0).unwrap(), Value::I64(20));
    assert_eq!(column.value(1).unwrap(), Value::I64(30));
    drop((column, charge));
    assert!(owner.upgrade().is_none());
    assert_eq!(memory.used_bytes(), 0);
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
    assert_eq!(batch.working_set_bytes(&memory).unwrap(), distinct_charges(batch.page_charges(), &memory).unwrap());
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
fn membership_build_key_reuses_an_independently_funded_column() {
    let memory = MemoryBudget::new(1 << 20);
    let mut source = ColumnBatch::settled(vec![Column::Values(vec![Value::I64(42); 2])], 2, memory.reserve(0).unwrap()).unwrap();
    let array: ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![Some(10), None]));
    let field = Arc::new(ArrowField::new("key", DataType::Int64, true));
    let logical = LogicalType::Scalar(ScalarType::I64).maybe();
    source.append_column(Column::Arrow { array: Arc::clone(&array), field, logical: Some(logical) },
        memory.reserve(1024).unwrap(), &memory).unwrap();
    let owner = Arc::downgrade(source.column_reservation(1));
    let key = source.membership_build_key(1, &memory).unwrap();
    drop(source);
    let [Column::Arrow { array: retained, .. }] = key.columns.as_slice() else {
        panic!("Expected one direct Arrow key");
    };
    assert!(Arc::ptr_eq(retained, &array));
    assert!(owner.upgrade().is_some());
    drop((key, array));
    assert!(owner.upgrade().is_none());
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn membership_build_key_detaches_wide_backing_and_keeps_nullable_date64() {
    use arrow_array::{Date64Array, RecordBatch, StringArray};
    use arrow_schema::Schema;

    let memory = MemoryBudget::new(3 << 20);
    let date = LogicalType::Scalar(ScalarType::DateMilli).maybe();
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("payload", DataType::Utf8, false),
        rtdb_columnar::to_field("key", &date).unwrap(),
    ]));
    let record = RecordBatch::try_new(schema, vec![
        Arc::new(StringArray::from(vec!["x".repeat(1 << 20), String::new()])),
        Arc::new(Date64Array::from(vec![Some(86_400_000), None])),
    ]).unwrap();
    let source = rtdb_columnar::OwnedBatch::new(record, &memory).unwrap();
    let owner = Arc::downgrade(source.reservation());
    let batch = ColumnBatch::from_owned(source).unwrap();
    let key = batch.membership_build_key(1, &memory).unwrap();
    drop(batch);
    assert!(owner.upgrade().is_none());
    let [Column::Arrow { array, logical, .. }] = key.columns.as_slice() else {
        panic!("Expected one direct Arrow key");
    };
    assert_eq!(array.data_type(), &DataType::Date64);
    assert_eq!(logical.as_ref(), Some(&date));
    assert_eq!(key.columns[0].value(0).unwrap(), Value::DateMilli(86_400_000));
    assert_eq!(key.columns[0].value(1).unwrap(), Value::Null);
    assert!(memory.used_bytes() < 64 * 1024);
    drop(key);
    assert_eq!(memory.used_bytes(), 0);
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
fn mixed_gather_borrows_repeated_text_and_preserves_bottom_metadata() {
    let memory = MemoryBudget::new(1 << 20);
    let text = Value::Utf8("x".repeat(64 * 1024));
    let evaluated = Column::Values(vec![text.clone(), Value::Undefined]);
    let bottom = Column::Arrow {
        array: Arc::new(NullArray::new(1)),
        field: Arc::new(rtdb_columnar::to_field("value", &LogicalType::Nothing).unwrap()),
        logical: Some(LogicalType::Nothing),
    };
    let batches = [
        ColumnBatch::settled(vec![evaluated], 2, memory.reserve(0).unwrap()).unwrap(),
        ColumnBatch::settled(vec![bottom], 1, memory.reserve(0).unwrap()).unwrap(),
    ];
    let scratch = memory.scope_with_limit(160 * 1024);
    let picks = [Some((0, 0)), None, Some((1, 0)), Some((0, 0))];
    let (column, charge) = gather_column_cells(|index| &batches[index], picks.into_iter(), picks.len(), 0, true, &scratch).unwrap();
    drop(batches);
    assert_eq!(column.value(0).unwrap(), text);
    assert!(matches!(column.arrow_cell(1), Some(None)));
    let (_, field, logical, _) = column.arrow_cell(2).unwrap().unwrap();
    assert!(!field.is_nullable());
    assert_eq!(logical, Some(&LogicalType::Nothing));
    assert_eq!(column.value(2).unwrap_err().code, "xtdb/zero-leg-read");
    assert_eq!(column.value(3).unwrap(), text);
    drop((column, charge));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn mixed_gather_packs_borrowed_records_timestamps_and_lists_in_pick_order() {
    use rtdb_types::{ListValue, StructFields, TimeUnit};

    let memory = MemoryBudget::new(1 << 20);
    let values = [
        Value::Struct(StructFields::from([("a".into(), Value::I64(42))])),
        Value::Timestamp { value: -1001, unit: TimeUnit::Millisecond, timezone: Some("Europe/Paris".into()) },
        Value::List(ListValue::from_f32(arrow_array::Float32Array::from(vec![None, Some(-0.0)]))),
        Value::Struct(StructFields::from([("b".into(), Value::Null)])),
    ];
    let batch = ColumnBatch::settled(vec![Column::Values(values.to_vec())], values.len(), memory.reserve(0).unwrap()).unwrap();
    let rows = [3, 1, 2, 0, 1];
    let picks = rows.into_iter().map(|row| Some((0, row)));
    let (column, charge) = gather_column_cells(|_| &batch, picks, rows.len(), 0, true, &memory).unwrap();
    drop(batch);
    for (position, source) in rows.into_iter().enumerate() {
        assert_eq!(column.value(position).unwrap(), values[source]);
    }
    drop((column, charge));
    assert_eq!(memory.used_bytes(), 0);
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
    let (Column::Arrow { array, .. }, _) = pack_evaluated_column(&mixed, &memory).unwrap() else {
        panic!("mixed widths share one Arrow column")
    };
    let child = array.as_any().downcast_ref::<ListArray>().unwrap().values();
    assert!(child.as_any().is::<UnionArray>());
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
    let len = evaluated.clone().count();
    let converted = if len != 0 {
        let _scratch = memory.reserve(len.checked_mul(size_of::<&Value>()).ok_or_else(size_error)?)?;
        let mut values = Vec::with_capacity(len);
        for (index, row) in evaluated {
            let Column::Values(column) = source(index).0 else { unreachable!("Only evaluated cells were selected") };
            values.push(column.get(row).ok_or_else(|| invalid("column-row", "Column row is outside its page"))?);
        }
        Some(pack_evaluated_cells(&values, memory, rtdb_columnar::copy_array_refs)?)
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
    take_arrow_rows(picks, count, retain_sources, memory, None)
}

fn take_arrow_rows<'a>(
    rows: impl Iterator<Item = Option<(&'a Column, &'a Arc<MemoryReservation>, usize)>>, count: usize,
    retain_sources: bool, memory: &Arc<MemoryBudget>, context: Option<&crate::QueryContext>,
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
    for (row, cell) in rows.enumerate() {
        if let Some(context) = context { context.check_cancelled_at(row)?; }
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
                let (source, offset) = column.cells.get(row).expect("A gather position is inside its source");
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
    gather_with_funded_columns(rows, sources_in, columns, memory, |position| Ok(derived(position)?.map(|column| (column, None))))
}

pub(crate) fn gather_with_funded_columns(rows: &[ResolvedRow<(u32, u32)>], sources_in: &[Arc<IndexBatch>], columns: &[Field],
    memory: &Arc<MemoryBudget>,
    mut derived: impl FnMut(usize) -> Result<Option<(Column, Option<Arc<MemoryReservation>>)>>) -> Result<ColumnBatch> {
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
    let base = columns.len().checked_mul(size_of::<Column>())
        .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
    let mut reservation = memory.reserve(base)?;
    let mut output = Vec::with_capacity(columns.len());
    let mut appended = Vec::new();
    for (position, field) in columns.iter().enumerate() {
        let (column, charge) = if let Some(derived) = derived(position)? {
            derived
        } else {
            let column = match field.name.as_str() {
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
            };
            (column, None)
        };
        let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("column-size", "A column page's charge is shared"))?;
        match charge {
            Some(charge) => {
                if appended.is_empty() {
                    let trailing = columns.len() - position;
                    let bytes = trailing.checked_mul(size_of::<Arc<MemoryReservation>>())
                        .and_then(|bytes| lease.bytes().checked_add(bytes)).ok_or_else(size_error)?;
                    lease.grow_to(bytes)?;
                    appended.reserve_exact(trailing);
                }
                appended.push(charge);
            }
            None if appended.is_empty() => {
                lease.grow_to(lease.bytes().checked_add(column.retained_bytes()?).ok_or_else(size_error)?)?;
            }
            None => appended.push(memory.reserve(column.retained_bytes()?)?),
        }
        output.push(column);
    }
    Ok(ColumnBatch { columns: output, len: rows.len(), reservation, appended })
}

#[test]
fn a_funded_gather_keeps_the_original_charge_through_replay() {
    let memory = MemoryBudget::new(1 << 20);
    let fields = ["_valid_from", "funded", "_system_to"].into_iter().enumerate().map(|(i, name)| Field {
        id: rtdb_plan::ColumnId(i as u32), name: name.into(), qualifier: None,
        qualified_only: false, unnamed: false, data_type: LogicalType::Null,
    }).collect::<Vec<_>>();
    let owner = memory.reserve(8192).unwrap();
    let original = Arc::downgrade(&owner);
    let mut funded = Some((Column::Arrow {
        array: Arc::new(NullArray::new(0)), field: Arc::new(ArrowField::new("funded", DataType::Null, true)),
        logical: Some(LogicalType::Null),
    }, Some(owner)));
    let output = gather_with_funded_columns(&[], &[], &fields, &memory, |i| {
        Ok(if i == 1 { funded.take() } else { None })
    }).unwrap();

    assert_eq!(Arc::as_ptr(output.column_reservation(1)), original.as_ptr());
    assert_eq!(memory.used_bytes(), 8192 + 256 + 3 * size_of::<Column>() + 2 * size_of::<Arc<MemoryReservation>>()
        + output.columns[0].retained_bytes().unwrap() + output.columns[2].retained_bytes().unwrap());
    let replay = output.duplicate_for_replay(&memory).unwrap();
    drop(output);
    assert_eq!(Arc::as_ptr(replay.column_reservation(1)), original.as_ptr());
    drop(replay);
    assert!(original.upgrade().is_none());
    assert_eq!(memory.used_bytes(), 0);
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
    let array = UnionArray::try_new(legs.clone(), arrow_buffer::ScalarBuffer::from(ids), Some(arrow_buffer::ScalarBuffer::from(offsets)), children)
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
fn contiguous_fragment_selection_preserves_its_funded_source() {
    let memory = MemoryBudget::new(1 << 20);
    let array = arrow_array::Int64Array::from(vec![None, Some(20), None, Some(40)]);
    let pointer = array.values()[1..].as_ptr();
    let field = Arc::new(ArrowField::new("col", DataType::Int64, true));
    let grant = memory.reserve(array.get_array_memory_size()).unwrap();
    let selected = SelectedColumn::new(vec![Some((Arc::new(array), field, None, Some(grant)))],
        Arc::new(vec![(0, 1), (0, 2), (0, 3), (0, 0)]));
    let original = memory.used_bytes();
    let ((source, _, _), rows) = selected.fragment_source(0..3).unwrap();
    let borrowed = source.slice(rows.start, rows.len());
    let (gathered, charge) = selected.fragment_vector(0..3, &memory).unwrap();
    let borrowed = borrowed.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
    assert_eq!(borrowed.values().as_ptr(), pointer);
    assert_eq!(borrowed.iter().collect::<Vec<_>>(), vec![Some(20), None, Some(40)]);
    assert_eq!(gathered.0.as_ref(), borrowed as &dyn Array);
    drop((gathered, charge));
    assert_eq!(memory.used_bytes(), original);
    assert!(selected.fragment_source(1..4).is_none());
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
    assert!(wire.as_slice().len() < width * 2);
    let frames = crate::fragment::decode_batches(wire.into_ipc_buffer(), &memory).unwrap();

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
        cells: SelectedRows::Picks(Arc::new(vec![(0, 0); len])),
    });
    let batch = ColumnBatch::settled(vec![selected], len, memory.reserve(256).unwrap()).unwrap();
    let mut wire = crate::fragment::FragmentBytes::new(&memory).unwrap();

    crate::fragment::encode_batch(&batch, &memory, &mut wire).unwrap();
    let frames = crate::fragment::decode_batches(wire.into_ipc_buffer(), &memory).unwrap();

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
    let mut decoded = crate::fragment::decode_batches(wire.into_ipc_buffer(), &memory).unwrap();
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
        cells: SelectedRows::Picks(Arc::new(vec![(0, 0), (1, 0)])),
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
    let payload = gathered.working_set_bytes(&memory).unwrap() - distinct_charges(gathered.page_charges(), &memory).unwrap();
    assert!(payload >= 1 << 20);
    let duplicate = ColumnBatch::settled(
        vec![gathered.columns[0].duplicate(), gathered.columns[0].duplicate()], 5, memory.reserve(256).unwrap(),
    ).unwrap();
    assert_eq!(duplicate.working_set_bytes(&memory).unwrap() - distinct_charges(duplicate.page_charges(), &memory).unwrap(), payload);
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
        cells: SelectedRows::Picks(Arc::new(vec![(0, 1)])),
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
        cells: SelectedRows::Picks(Arc::new(vec![(0, 0)])),
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
        cells: SelectedRows::Picks(Arc::new(vec![(0, 0)])),
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
    owner: &Arc<MemoryReservation>) -> Column {
    if !nested_layout(array.data_type()) {
        return Column::Arrow { array, field, logical };
    }
    let cells = SelectedRows::Span(0..array.len());
    let sources = Arc::new(vec![Some((array, field, logical, Some(Arc::clone(owner))))]);
    Column::Selected(SelectedColumn { sources, cells })
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
        return column.relayout(target, field, memory, false);
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

pub(crate) fn relayout_spatial_probe_output(
    column: &Column, target: &LogicalType, field: &ArrowField, memory: &Arc<MemoryBudget>,
) -> Result<Option<(ArrayRef, Arc<MemoryReservation>)>> {
    match column {
        Column::Selected(column) => column.relayout(target, field, memory, true),
        _ => relayout_output(column, target, field, memory),
    }
}

impl SelectedColumn {
    fn relayout(&self, target: &LogicalType, field: &ArrowField, memory: &Arc<MemoryBudget>, compact_sources: bool)
        -> Result<Option<(ArrayRef, Arc<MemoryReservation>)>> {
        let base = self.cells.len().checked_mul(64).and_then(|bytes| self.sources.len().checked_mul(128).and_then(|extra| bytes.checked_add(extra)))
            .and_then(|bytes| field.size().checked_mul(8).and_then(|extra| bytes.checked_add(extra))).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
        let Ok(mut scratch) = memory.reserve(base) else { return Ok(None); };
        let mut used = vec![false; self.sources.len()];
        for (source, _) in self.cells.iter() { used[source] = true; }
        let mut positions = vec![0; self.sources.len()];
        let mut arrays = Vec::new();
        let mut row_indices = compact_sources.then(|| self.cells.iter().map(|(_, row)| row).collect::<Vec<_>>());
        let null = ArrowField::new(field.name(), DataType::Null, true);
        for (position, source) in self.sources.iter().enumerate().filter(|(position, _)| used[*position]) {
            let (source, mut array, logical) = match source {
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
                if let Some(row_indices) = &mut row_indices
                    && self.sources[position].is_some()
                {
                    let mut picks = Vec::new();
                    for (index, (source, row)) in self.cells.iter().enumerate() {
                        if source == position {
                            row_indices[index] = picks.len();
                            picks.push((0, row));
                        }
                    }
                    let lease = Arc::get_mut(&mut scratch).expect("a selection owns its scratch");
                    let bound = picks.iter().try_fold(lease.bytes(), |bytes, &(_, row)| {
                        rtdb_columnar::estimate_copy_cell_bytes(source, array.as_ref(), row)?.checked_mul(4)
                            .and_then(|size| bytes.checked_add(size)).ok_or_else(size_error)
                    })?;
                    if lease.grow_to(bound).is_err() { return Ok(None); }
                    let Ok(compact) = copy_selected_cells(&[array.as_ref()], &picks) else { return Ok(None); };
                    array = compact;
                }
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
        let picks = self.cells.iter().enumerate().map(|(index, (source, row))| {
            let row = if self.sources[source].is_some() {
                row_indices.as_ref().map_or(row, |rows| rows[index])
            } else { 0 };
            (positions[source], row)
        }).collect::<Vec<_>>();
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

pub(crate) fn stable_read_layout(logical: &LogicalType, field: &ArrowField) -> bool {
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

fn readable_mixed_unnest_layout(logical: &LogicalType, field: &ArrowField, lists: bool) -> bool {
    if !field.metadata().is_empty() { return false; }
    let logical = match logical { LogicalType::Maybe(inner) => inner.as_ref(), ty => ty };
    match field.data_type() {
        DataType::List(child) if lists => {
            let LogicalType::List { kind: ListKind::List, element } = logical else { return false; };
            readable_mixed_unnest_layout(element, child, false)
        }
        DataType::Union(fields, arrow_schema::UnionMode::Dense) => fields.iter().all(|(_, child)| {
            !matches!(child.data_type(), DataType::Union(..))
                && rtdb_columnar::from_field(child).is_ok_and(|actual| {
                    // A named leg can override the physical type in a logical read of the same cell.
                    let name_agrees = match logical {
                        LogicalType::Union(legs) => legs.iter().all(|leg| {
                            !rtdb_columnar::leg_name(leg).is_ok_and(|name| name == *child.name())
                                || *leg == actual || leg.clone().maybe() == actual
                        }),
                        _ => true,
                    };
                    name_agrees && readable_mixed_unnest_layout(&actual, child, lists)
                })
        }),
        DataType::Null => field.is_nullable(),
        DataType::Boolean | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => true,
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

impl ColumnBatch {
    pub(crate) fn slice(mut self, start: usize, count: usize, memory: &Arc<MemoryBudget>) -> Result<Self> {
        if count == self.len { return Ok(self); }
        let end = start + count;
        let scratch = self.columns.iter().filter(|column| matches!(column, Column::Selected(SelectedColumn { cells: SelectedRows::Picks(_), .. }))).count()
            .checked_mul(count).and_then(|cells| cells.checked_mul(std::mem::size_of::<(usize, usize)>())).ok_or_else(size_error)?;
        let _selection = memory.reserve(scratch)?;
        for column in &mut self.columns {
            match column {
                Column::Arrow { array, .. } => *array = array.slice(start, count),
                Column::Selected(column) => column.cells = column.cells.slice(start..end),
                Column::Values(values) => { values.truncate(end); values.drain(..start); }
            }
        }
        self.len = count;
        Ok(self)
    }

    /// Keeps the rows `mask` selects, column by column.
    pub(crate) fn select(self, mask: &[bool], memory: &Arc<MemoryBudget>) -> Result<Self> {
        if mask.len() == self.len && mask.iter().all(|&keep| keep) { return Ok(self); }
        let selection = PredicateSelection::from_bits(mask.iter().copied(), memory)?;
        self.select_predicate(&selection, memory)
    }

    fn filter_slice_fits(&self, rows: Range<usize>, memory: &Arc<MemoryBudget>) -> bool {
        if self.columns.iter().any(|column| match column {
            Column::Arrow { array, field, .. } => array.len() < self.len || array.data_type() != field.data_type()
                || !scalar_layout(array.data_type()) || array.data_type() == &DataType::Null || array.nulls().is_some(),
            Column::Selected(_) => false,
            Column::Values(_) => true,
        }) {
            return false;
        }
        let scratch = self.columns.len().saturating_mul(256);
        let Ok(_scratch) = memory.reserve(scratch) else { return false; };
        let views = self.columns.iter().filter_map(|column| match column {
            Column::Arrow { array, .. } => Some(array.slice(rows.start, rows.len())),
            _ => None,
        }).collect::<Vec<_>>();
        let Some(visible) = rtdb_columnar::visible_buffer_bytes(&views) else { return false; };
        let Ok(backing) = distinct_charges(self.page_charges(), memory) else { return false; };
        let backing = backing
            .max(views.iter().map(|array| array.get_buffer_memory_size()).max().unwrap_or(0));
        backing <= visible.saturating_mul(2).saturating_add(4096)
    }

    pub(crate) fn select_predicate(self, selection: &PredicateSelection, memory: &Arc<MemoryBudget>) -> Result<Self> {
        selection.validate_len(self.len)?;
        let selected = selection.count;
        if selected == self.len { return Ok(self); }
        let span = selection.contiguous_span();
        if let Some(rows) = &span
            && self.filter_slice_fits(rows.clone(), memory)
        {
            return self.slice(rows.start, rows.len(), memory);
        }
        let compact = if span.is_some() { 0 } else {
            self.columns.iter().filter(|column| matches!(column, Column::Selected(selected) if !matches!(selected.cells, SelectedRows::Picks(_)))).count()
        };
        let picks = compact.checked_mul(selected).and_then(|cells| cells.checked_mul(size_of::<(usize, usize)>())).ok_or_else(size_error)?;
        let bytes = distinct_charges(self.page_charges(), memory)?;
        let reservation = memory.reserve(bytes.saturating_add(picks))?;
        let predicate = arrow_select::filter::FilterBuilder::new(&selection.mask).build();
        let columns = self.columns.into_iter().map(|column| Ok(match column {
            Column::Arrow { array, field, logical } => Column::Arrow {
                array: predicate.filter(array.as_ref()).map_err(|error| invalid("column-filter", error))?, field, logical,
            },
            Column::Selected(column) => Column::Selected(column.select(selection, span.as_ref())),
            Column::Values(values) => Column::Values(values.into_iter().zip(selection.mask.values().iter())
                .filter_map(|(value, keep)| keep.then_some(value)).collect()),
        })).collect::<Result<Vec<_>>>()?;
        Self::settled(columns, selected, reservation)
    }
}

pub(crate) struct PredicateSelection {
    mask: BooleanArray,
    count: usize,
    _reservation: Arc<MemoryReservation>,
}

impl PredicateSelection {
    fn bitmap_bound(len: usize) -> usize { len.div_ceil(8) + 256 }

    fn settled(mask: BooleanArray, count: usize, mut reservation: Arc<MemoryReservation>) -> Result<Self> {
        let lease = Arc::get_mut(&mut reservation).expect("The filter bitmap owns its reservation");
        let bytes = mask.get_array_memory_size();
        if bytes <= lease.bytes() { lease.shrink_to(bytes)?; } else { lease.grow_to(bytes)?; }
        Ok(Self { mask, count, _reservation: reservation })
    }

    fn from_bits(bits: impl ExactSizeIterator<Item = bool>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(Self::bitmap_bound(bits.len()))?;
        let values = bits.collect::<BooleanBuffer>();
        let count = values.count_set_bits();
        Self::settled(BooleanArray::new(values, None), count, reservation)
    }

    pub(crate) fn build(
        len: usize, memory: &Arc<MemoryBudget>, fill: impl FnOnce(&mut BooleanBufferBuilder) -> Result<()>,
    ) -> Result<Self> {
        let reservation = memory.reserve(Self::bitmap_bound(len))?;
        let mut bits = BooleanBufferBuilder::new(len);
        fill(&mut bits)?;
        let values = bits.finish();
        debug_assert_eq!(values.len(), len);
        let count = values.count_set_bits();
        Self::settled(BooleanArray::new(values, None), count, reservation)
    }

    pub(crate) fn is_empty(&self) -> bool { self.count == 0 }

    pub(crate) fn validate_len(&self, len: usize) -> Result<()> {
        if self.mask.len() != len {
            return Err(invalid("column-filter", "Filter predicate length differs from its batch"));
        }
        Ok(())
    }

    fn from_arrow(mask: BooleanArray, mut reservation: Arc<MemoryReservation>) -> Result<Self> {
        let count = mask.true_count();
        let mask = if mask.null_count() == 0 || count == mask.values().count_set_bits() {
            // Search's builder leaves the value bits clear in null slots.
            BooleanArray::new(mask.into_parts().0, None)
        } else {
            let copy = reservation.budget().reserve(Self::bitmap_bound(mask.len()))?;
            let values = arrow_select::filter::prep_null_mask_filter(&mask);
            drop(mask);
            reservation = copy;
            values
        };
        Self::settled(mask, count, reservation)
    }

    pub(crate) fn contains(&self, row: usize) -> bool { self.mask.value(row) }

    fn contiguous_span(&self) -> Option<Range<usize>> {
        let (start, end) = self.mask.values().set_slices().next()?;
        (end - start == self.count).then_some(start..end)
    }
}

#[test]
fn a_boolean_predicate_selection_keeps_only_valid_true_bits() {
    let memory = MemoryBudget::new(1 << 20);
    let logical = LogicalType::Scalar(ScalarType::Bool).maybe();
    let field = Field { id: rtdb_plan::ColumnId(0), name: "predicate".into(), qualifier: None,
        qualified_only: false, unnamed: false, data_type: logical.clone() };
    let column = field.expression();
    let schema = rtdb_plan::Schema::new(vec![field]).unwrap();
    let bits = [true, true, false, true].into_iter().collect::<BooleanBuffer>();
    let valid = [true, false, true, true].into_iter().collect::<BooleanBuffer>();
    let batch = ColumnBatch::settled(vec![Column::Arrow {
        array: Arc::new(BooleanArray::new(bits, Some(arrow_buffer::NullBuffer::new(valid)))),
        field: Arc::new(ArrowField::new("predicate", DataType::Boolean, true)), logical: Some(logical),
    }], 4, memory.reserve(0).unwrap()).unwrap();
    for (predicate, expected) in [
        (column.clone(), [true, false, false, true]),
        (Expr::unary(rtdb_plan::UnaryOp::Not, column), [false, false, true, false]),
    ] {
        let selection = functions::Predicate::bind(&predicate, &schema).unwrap()
            .selection(&batch, rtdb_expr::Parameters::Values(&[]), &memory, || Ok(())).unwrap().unwrap();
        let actual: [bool; 4] = std::array::from_fn(|row| selection.contains(row));
        assert_eq!(actual, expected);
    }
}

/// Filters batches, evaluating the predicate over whole columns where its plan-time binding against the schema allows,
/// and row by row, exactly as the row filter does, otherwise.
pub(crate) struct BatchFilter {
    input: BoxBatchCursor, schema: Arc<rtdb_plan::Schema>, stages: Arc<[rtdb_plan::Expr]>,
    compiled: Arc<[Option<functions::Predicate>]>,
    admissions: Option<Arc<[crate::compiled::Admission]>>, execution: crate::context::Execution,
    stage_analysis: Vec<Option<Arc<crate::analysis::AnalysisNode>>>,
    scalar_fallbacks: Option<u64>,
}
impl BatchFilter {
    pub fn new(input: BoxBatchCursor, schema: rtdb_plan::Schema, predicate: rtdb_plan::Expr, execution: crate::context::Execution) -> Self {
        let compiled: Arc<[Option<functions::Predicate>]> = Arc::from([functions::Predicate::bind(&predicate, &schema)]);
        let scalar_fallbacks = operator_evidence_enabled(&execution.context).then_some(0);
        Self { input, schema: Arc::new(schema), stages: vec![predicate].into(), compiled, admissions: None, execution, stage_analysis: Vec::new(), scalar_fallbacks }
    }
    /// Applies a compiled plan's predicates, sharing their plan-time bindings and admissions with every execution of that plan.
    pub(crate) fn admitted(input: BoxBatchCursor, predicates: &crate::compiled::Predicates, execution: crate::context::Execution) -> Self {
        let scalar_fallbacks = operator_evidence_enabled(&execution.context).then_some(0);
        Self { input, schema: Arc::clone(&predicates.schema), stages: Arc::clone(&predicates.stages),
            compiled: Arc::clone(&predicates.compiled),
            admissions: Some(Arc::clone(&predicates.admissions)), execution, stage_analysis: Vec::new(), scalar_fallbacks }
    }
    /// Measures each stage, innermost first, as the filter it was fused from; `None` leaves a stage to its operator's wrapper.
    pub fn observing_stages(mut self, stage_analysis: Vec<Option<Arc<crate::analysis::AnalysisNode>>>) -> Self {
        debug_assert_eq!(stage_analysis.len(), self.stages.len());
        self.stage_analysis = stage_analysis;
        self
    }
    /// A fused stage's time covers its input and the stages below it, as its own operator's would.
    fn observe_stages(&self, from: usize, to: usize, started: std::time::Instant, rows: Option<usize>) -> Result<()> {
        let Some(nodes) = self.stage_analysis.get(from..to.min(self.stage_analysis.len())) else { return Ok(()) };
        for node in nodes.iter().flatten() { node.observe_stage(started.elapsed(), rows)?; }
        Ok(())
    }
}
#[async_trait::async_trait]
impl BatchCursor for BatchFilter {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        let stages = self.stages.len();
        'batches: loop {
            let started = std::time::Instant::now();
            let Some(mut batch) = poll_nested_batch(self.input.as_mut()).await? else {
                self.observe_stages(0, stages, started, None)?;
                return Ok(None);
            };
            for (index, predicate) in self.stages.iter().enumerate() {
                let admission = self.admissions.as_ref().map(|admissions| &admissions[index]);
                let selection = admitted_predicate_selection_counted(
                    &batch, predicate, &self.schema, &self.execution, self.compiled[index].as_ref(), admission,
                    self.scalar_fallbacks.as_mut(),
                ).await?;
                if selection.count == 0 {
                    self.observe_stages(index, stages, started, None)?;
                    continue 'batches;
                }
                batch = batch.select_predicate(&selection, &self.execution.context.memory)?;
                self.observe_stages(index, index + 1, started, Some(batch.len))?;
            }
            return Ok(Some(batch));
        }
    }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) {
        self.input.trace_attributes(span);
        if let Some(count) = self.scalar_fallbacks {
            OperatorCounter::PredicateScalarFallbacks.record(span, count);
        }
    }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
}

/// The rows `predicate` holds at, `Ok(None)` where the row evaluator computes it.
pub(crate) fn typed_predicate_mask(
    batch: &ColumnBatch, predicate: &rtdb_plan::Expr, schema: &rtdb_plan::Schema, parameters: rtdb_expr::Parameters<'_>,
) -> Result<Option<Vec<bool>>> {
    let Some(truth) = functions::truth(predicate, schema, batch, parameters)? else { return Ok(None) };
    Ok(Some(truth.into_iter().map(|value| value == Some(true)).collect()))
}

pub(crate) async fn predicate_selection(
    batch: &ColumnBatch, predicate: &rtdb_plan::Expr, schema: &rtdb_plan::Schema, execution: &crate::context::Execution,
) -> Result<PredicateSelection> {
    let compiled = functions::Predicate::bind(predicate, schema);
    admitted_predicate_selection(batch, predicate, schema, execution, compiled.as_ref(), None).await
}

/// As `predicate_selection`, using `compiled`'s plan-time binding and skipping the type admission of a kernel-evaluated
/// predicate that `admission` already passed.
pub(crate) async fn admitted_predicate_selection(
    batch: &ColumnBatch, predicate: &rtdb_plan::Expr, schema: &rtdb_plan::Schema, execution: &crate::context::Execution,
    compiled: Option<&functions::Predicate>, admission: Option<&crate::compiled::Admission>,
) -> Result<PredicateSelection> {
    admitted_predicate_selection_counted(batch, predicate, schema, execution, compiled, admission, None).await
}

async fn admitted_predicate_selection_counted(
    batch: &ColumnBatch, predicate: &rtdb_plan::Expr, schema: &rtdb_plan::Schema, execution: &crate::context::Execution,
    compiled: Option<&functions::Predicate>, admission: Option<&crate::compiled::Admission>, scalar_fallbacks: Option<&mut u64>,
) -> Result<PredicateSelection> {
    execution.context.check_cancelled()?;
    let params = execution.parameters.evaluator();
    let memory = &execution.context.memory;
    if search::shaped(predicate)
        && let Some((Column::Arrow { array, .. }, reservation)) = search::funded_column(predicate, schema, batch, params, &execution.context)?
        && let Some(values) = array.as_any().downcast_ref::<BooleanArray>()
    {
        let values = values.clone();
        drop(array);
        return PredicateSelection::from_arrow(values, reservation);
    }
    let prepare_typed = || -> Result<()> {
        if batch.columns.iter().any(|column| matches!(column, Column::Selected(_))) && !admission.is_some_and(crate::compiled::Admission::admitted) {
            let metadata = execution.scope()?;
            execution.prepare_expression(predicate, schema, &metadata)?;
            if let Some(admission) = admission { admission.admit(); }
        }
        Ok(())
    };
    if let Some(compiled) = compiled && let Some(selection) = compiled.selection(batch, params, memory, prepare_typed)? {
        return Ok(selection);
    }
    let metadata = execution.scope()?;
    let invocation = execution.prepare_expression(predicate, schema, &metadata)?;
    let scope = execution.scope()?;
    let reservation = memory.reserve(PredicateSelection::bitmap_bound(batch.len))?;
    let mut mask = BooleanBufferBuilder::new(batch.len);
    let mut count = 0;
    if let Some(fallbacks) = scalar_fallbacks && batch.len != 0 {
        *fallbacks = fallbacks.saturating_add(1);
    }
    for row in 0..batch.len {
        let input = BatchRow { batch, row, admit_dates: false };
        let value = execution.evaluate_invocation_row(
            &scope, &invocation, rtdb_expr::ExpressionRow::Borrowed(&input),
        ).await?;
        let keep = rtdb_expr::is_true(&value)?;
        mask.append(keep);
        count += usize::from(keep);
        scope.reset()?;
    }
    PredicateSelection::settled(BooleanArray::new(mask.finish(), None), count, reservation)
}

/// Each row's three-valued truth under the predicate, through the same evaluation tiers as `predicate_selection`,
/// keeping unknown (null) apart from false. The charge for the truths lasts as long as the returned reservation.
pub(crate) async fn predicate_truth(
    batch: &ColumnBatch, predicate: &rtdb_plan::Expr, schema: &rtdb_plan::Schema, execution: &crate::context::Execution,
) -> Result<(Vec<Option<bool>>, Arc<MemoryReservation>)> {
    execution.context.check_cancelled()?;
    let params = execution.parameters.evaluator();
    let memory = &execution.context.memory;
    let reservation = memory.reserve(batch.len.checked_mul(size_of::<Option<bool>>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?)?;
    let from_arrow = |values: &BooleanArray| (0..values.len()).map(|row| values.is_valid(row).then(|| values.value(row))).collect();
    if search::shaped(predicate)
        && let Some((Column::Arrow { array, .. }, _charge)) = search::funded_column(predicate, schema, batch, params, &execution.context)?
        && let Some(values) = array.as_any().downcast_ref::<BooleanArray>()
    {
        return Ok((from_arrow(values), reservation));
    }
    let prepare_typed = || -> Result<()> {
        if batch.columns.iter().any(|column| matches!(column, Column::Selected(_))) {
            let metadata = execution.scope()?;
            execution.prepare_expression(predicate, schema, &metadata)?;
        }
        Ok(())
    };
    if let Some(compiled) = functions::Predicate::bind(predicate, schema) && let Some(truth) = compiled.truth(batch, params)? {
        prepare_typed()?;
        return Ok((truth.into_iter().collect(), reservation));
    }
    let metadata = execution.scope()?;
    let invocation = execution.prepare_expression(predicate, schema, &metadata)?;
    let scope = execution.scope()?;
    let mut truths = Vec::with_capacity(batch.len);
    for row in 0..batch.len {
        let input = BatchRow { batch, row, admit_dates: false };
        let value = execution.evaluate_invocation_row(&scope, &invocation, rtdb_expr::ExpressionRow::Borrowed(&input)).await?;
        truths.push(if matches!(value, Value::Null | Value::Undefined) { None } else { Some(rtdb_expr::is_true(&value)?) });
        scope.reset()?;
    }
    Ok((truths, reservation))
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Float32Array, Float64Array, Int64Array, StringArray};
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

    #[test]
    fn arrow_predicate_reuses_clear_null_bits_and_masks_set_null_bits() {
        use arrow_buffer::NullBuffer;

        let memory = MemoryBudget::new(4096);
        for null_bit in [false, true] {
            let predicate = BooleanArray::new(
                BooleanBuffer::from(vec![true, false, true, null_bit, true, true]),
                Some(NullBuffer::from(vec![true, true, true, false, true, true])),
            ).slice(1, 4);
            let input = predicate.values().inner().as_ptr();
            let reservation = memory.reserve(predicate.get_array_memory_size()).unwrap();
            let predicate = PredicateSelection::from_arrow(predicate, reservation).unwrap();

            assert_eq!(predicate.mask.iter().collect::<Vec<_>>(), [Some(false), Some(true), Some(false), Some(true)]);
            assert_eq!(predicate.count, 2);
            assert_eq!(predicate.contiguous_span(), None);
            assert_eq!(predicate.mask.values().inner().as_ptr() == input, !null_bit);
            assert_eq!(memory.used_bytes(), predicate.mask.get_array_memory_size());
        }
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn contiguous_filter_shares_arrow_owner_and_composes_a_selected_span() {
        let memory = MemoryBudget::new(1 << 20);
        let payload = Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5]));
        let input = payload.values().as_ptr();
        let field = Arc::new(ArrowField::new("v", DataType::Int64, false));
        let logical = Some(LogicalType::Scalar(ScalarType::I64));
        let columns = vec![
            Column::Arrow { array: payload.clone(), field: Arc::clone(&field), logical: logical.clone() },
            Column::Selected(SelectedColumn {
                sources: Arc::new(vec![Some((payload, field, logical, None))]),
                cells: SelectedRows::Span(0..6),
            }),
        ];
        let batch = ColumnBatch::settled(columns, 6, memory.reserve(0).unwrap()).unwrap()
            .pack_evaluated(&memory).unwrap().slice(1, 4, &memory).unwrap();
        let owner = Arc::downgrade(batch.column_reservation(0));

        let filtered = batch.select(&[false, true, true, false], &memory).unwrap();

        let [Column::Arrow { array, .. }, Column::Selected(selection)] = filtered.columns.as_slice() else {
            panic!("Expected the original column layouts");
        };
        let values = array.as_any().downcast_ref::<Int64Array>().unwrap().values();
        assert_eq!(values.as_ptr(), input.wrapping_add(2));
        assert_eq!(values.as_ref(), &[2, 3]);
        assert!(matches!(&selection.cells, SelectedRows::Span(rows) if *rows == (2..4)));
        assert_eq!(filtered.columns[1].value(1).unwrap(), Value::I64(3));
        assert_eq!(Arc::as_ptr(filtered.column_reservation(0)), owner.as_ptr());
        drop(filtered);
        assert!(owner.upgrade().is_none());
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn contiguous_filter_detaches_a_small_run_between_large_payloads() {
        let memory = MemoryBudget::new(1 << 20);
        let wide = "x".repeat(1 << 16);
        let column = arrow(Arc::new(StringArray::from(vec![wide.as_str(), "a", "b", wide.as_str()])), ScalarType::Utf8);
        let batch = ColumnBatch::settled(vec![column], 4, memory.reserve(0).unwrap()).unwrap();
        let owner = Arc::downgrade(batch.column_reservation(0));

        let filtered = batch.select(&[false, true, true, false], &memory).unwrap();

        assert!(owner.upgrade().is_none());
        let Column::Arrow { array, .. } = &filtered.columns[0] else { panic!("Expected a compact Arrow column"); };
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.value_offsets(), &[0, 1, 2]);
        assert_eq!(strings.value_data(), b"ab");
        assert!(memory.used_bytes() < 4096);
    }

    #[test]
    fn contiguous_filter_preserves_null_payloads_in_the_compact_layout() {
        use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer};

        let memory = MemoryBudget::new(1 << 20);
        let strings = StringArray::new(
            OffsetBuffer::new(vec![0, 1, 3, 4].into()),
            Buffer::from(b"axxb".to_vec()),
            Some(NullBuffer::from(vec![true, false, true])),
        );
        let batch = ColumnBatch::settled(vec![arrow(Arc::new(strings), ScalarType::Utf8)], 3, memory.reserve(0).unwrap()).unwrap();

        let filtered = batch.select(&[false, true, true], &memory).unwrap();

        let Column::Arrow { array, .. } = &filtered.columns[0] else { panic!("Expected a compact Arrow column"); };
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.value_offsets(), &[0, 2, 3]);
        assert_eq!(strings.value_data(), b"xxb");
        assert_eq!(strings.iter().collect::<Vec<_>>(), [None, Some("b")]);
    }

    #[test]
    fn disjoint_filter_keeps_selected_and_owned_rows_in_input_order() {
        let memory = MemoryBudget::new(1 << 20);
        let batch = ColumnBatch::settled(vec![
            Column::Selected(SelectedColumn {
                sources: Arc::new(vec![Some((
                    Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])),
                    Arc::new(ArrowField::new("a", DataType::Int64, false)),
                    Some(LogicalType::Scalar(ScalarType::I64)), None,
                ))]),
                cells: SelectedRows::Span(1..5),
            }),
            Column::Values(vec![Value::I64(10), Value::I64(20), Value::I64(30), Value::I64(40)]),
        ], 4, memory.reserve(0).unwrap()).unwrap();

        let filtered = batch.select(&[true, false, false, true], &memory).unwrap();

        assert_eq!(filtered.len, 2);
        for (column, values) in filtered.columns.iter().zip([[1, 4], [10, 40]]) {
            assert_eq!(column.value(0).unwrap(), Value::I64(values[0]));
            assert_eq!(column.value(1).unwrap(), Value::I64(values[1]));
        }
    }

    #[test]
    fn fixed_size_unnest_checks_the_layout_and_borrows_funded_selected_children() {
        use arrow_array::{RecordBatch, types::Int64Type};

        let memory = MemoryBudget::new(1 << 20);
        let catalog = crate::CatalogSnapshot::new("xtdb", [("xtdb".into(), crate::DatabaseSnapshot {
            partitions: vec![],
            storage: Arc::new(rtdb_storage::cache::BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
        })].into(), false).unwrap();
        let context = crate::QueryContext::new(catalog, Arc::clone(&memory), Timestamp(0), "UTC", Default::default(),
            &tokio_util::sync::CancellationToken::new()).unwrap();
        let baseline = memory.used_bytes();
        let item = Arc::new(ArrowField::new("item", DataType::Int64, false));
        let lists = FixedSizeListArray::new(item, 2, Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])), None);
        let field = Arc::new(ArrowField::new("tags", lists.data_type().clone(), false));
        let schema = Arc::new(arrow_schema::Schema::new(vec![Arc::clone(&field)]));
        let record = RecordBatch::try_new(schema, vec![Arc::new(lists)]).unwrap();
        let owned = rtdb_columnar::OwnedBatch::new(record, &memory).unwrap();
        let array = owned.batch().column(0);
        let owner = Arc::downgrade(owned.reservation());
        let lists = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        let child = Arc::downgrade(lists.values());
        let logical = rtdb_columnar::from_field(&field).unwrap();
        let selected = SelectedColumn {
            sources: Arc::new(vec![Some((Arc::clone(array), field, Some(logical.clone()), Some(Arc::clone(owned.reservation()))))]),
            cells: SelectedRows::Picks(Arc::new(vec![(0, 2), (0, 1)])),
        };
        let batch = ColumnBatch::settled(vec![Column::Selected(selected)], 2, memory.reserve(4096).unwrap()).unwrap();

        let pressure = memory.reserve(memory.available_bytes()).unwrap();
        assert!(matches!(batch.list_expansion(0, Some(&logical), false, false, &context),
            Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted)));
        drop(pressure);
        let wrong_width = LogicalType::List {
            kind: ListKind::FixedSizeList(1), element: Box::new(LogicalType::Scalar(ScalarType::I64)),
        };
        assert!(batch.list_expansion(0, Some(&wrong_width), false, false, &context).unwrap().is_none());
        let empty_variable = ListArray::from_iter_primitive::<Int64Type, _, _>([Some(Vec::<Option<i64>>::new())]);
        let field = Arc::new(ArrowField::new("tags", empty_variable.data_type().clone(), true));
        let other = ColumnBatch::settled(vec![Column::Arrow { array: Arc::new(empty_variable), field, logical: None }],
            1, memory.reserve(4096).unwrap()).unwrap();
        assert!(other.list_expansion(0, Some(&logical.clone().maybe()), false, false, &context).unwrap().is_none());
        drop(other);

        let expansion = batch.list_expansion(0, Some(&logical), false, false, &context).unwrap().unwrap();
        let elements = expansion.elements(&expansion.runs);
        drop((owned, batch));

        assert!(owner.upgrade().is_some());
        for (row, expected) in [5, 6, 3, 4].into_iter().enumerate() {
            let (array, ..) = elements.arrow_cell(row).unwrap().unwrap();
            assert!(std::ptr::eq(array, child.upgrade().unwrap().as_ref()));
            assert_eq!(elements.value(row).unwrap(), Value::I64(expected));
        }
        drop((elements, expansion));
        assert!(owner.upgrade().is_none());
        assert_eq!(memory.used_bytes(), baseline);
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
    async fn cosine_cases_keep_double_bits_and_null_only_types_within_the_page_charge() {
        let len = 4096;
        let memory = MemoryBudget::new(1 << 24);
        let catalog = crate::CatalogSnapshot::new("xtdb", [("xtdb".into(), crate::DatabaseSnapshot {
            partitions: vec![],
            storage: Arc::new(rtdb_storage::cache::BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
        })].into(), false).unwrap();
        let context = crate::QueryContext::new(catalog, Arc::clone(&memory), Timestamp(0), "UTC", Default::default(), &tokio_util::sync::CancellationToken::new()).unwrap();
        let execution = crate::context::Execution { context: Arc::clone(&context), parameters: Arc::new(crate::parameters::BoundParameters::infer(Vec::new(), &memory).unwrap()),
            outer: crate::context::OuterRows::empty(&memory).unwrap(), bindings: None, analysis: None };
        let baseline = memory.used_bytes();
        let batch = ColumnBatch::settled(vec![], len, memory.reserve(0).unwrap()).unwrap();
        let input = single_batch(Some(batch), context);
        let double = LogicalType::Scalar(ScalarType::F64);
        let score = Expr::Call { name: "cosine_distance".into(), args: vec![Expr::Literal(Value::Null); 2], return_type: double.clone().maybe() };
        let projections = [Value::F64(-0.0), Value::Null].into_iter().map(|result| Expr::Case {
            branches: vec![
                (Expr::Literal(Value::Bool(false)), score.clone()),
                (Expr::Literal(Value::Bool(true)), Expr::Case {
                    branches: vec![(Expr::Literal(Value::Bool(true)), Expr::Literal(result))],
                    otherwise: Box::new(score.clone()),
                }),
            ],
            otherwise: Box::new(Expr::Literal(Value::Null)),
        }).collect();
        let mut cursor = BatchExpressions::new(input, Schema::default(), projections, execution.clone());
        let page = cursor.next_batch().await.unwrap().unwrap();
        let [
            Column::Arrow { array: doubles, field, logical: Some(ty) },
            Column::Arrow { array: nulls, logical: Some(LogicalType::Null), .. },
        ] = page.columns.as_slice() else {
            panic!("Expected a double column and a null column");
        };
        assert_eq!(*ty, double);
        assert!(!field.is_nullable());
        let doubles = doubles.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(doubles.len(), len);
        assert_eq!(doubles.null_count(), 0);
        assert!(doubles.values().iter().all(|value| value.to_bits() == (-0.0f64).to_bits()));
        assert_eq!(nulls.data_type(), &DataType::Null);
        assert_eq!(nulls.len(), len);
        assert!(memory.peak_bytes() < 2 * len * std::mem::size_of::<Value>());
        drop(page);
        drop(cursor);
        assert_eq!(memory.used_bytes(), baseline);
    }

    #[tokio::test]
    async fn fixed_width_cases_preserve_bits_nulls_and_branch_demand() {
        let len = 192;
        let memory = MemoryBudget::new(1 << 24);
        let catalog = crate::CatalogSnapshot::new("xtdb", [("xtdb".into(), crate::DatabaseSnapshot {
            partitions: vec![],
            storage: Arc::new(rtdb_storage::cache::BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
        })].into(), false).unwrap();
        let context = crate::QueryContext::new(catalog, Arc::clone(&memory), Timestamp(0), "UTC", Default::default(),
            &tokio_util::sync::CancellationToken::new()).unwrap();
        let execution = crate::context::Execution {
            context: Arc::clone(&context),
            parameters: Arc::new(crate::parameters::BoundParameters::infer(Vec::new(), &memory).unwrap()),
            outer: crate::context::OuterRows::empty(&memory).unwrap(),
            bindings: None,
            analysis: None,
        };
        let baseline = memory.used_bytes();
        let schema = Schema::new(vec![field(0, ScalarType::I64)]).unwrap();
        let selector = schema.fields()[0].expression();
        let input = |rows: usize| {
            let array = Int64Array::from_iter_values((0..rows).map(|row| (row % 3) as i64));
            let batch = ColumnBatch::settled(
                vec![arrow(Arc::new(array), ScalarType::I64)],
                rows,
                memory.reserve(0).unwrap(),
            ).unwrap();
            single_batch(Some(batch), Arc::clone(&context))
        };
        let case = |branches: Vec<(i64, Expr)>, otherwise| Expr::SimpleCase {
            operand: Box::new(selector.clone()),
            branches: branches.into_iter().map(|(value, result)| (Expr::Literal(Value::I64(value)), result)).collect(),
            otherwise: Box::new(otherwise),
        };
        let cast = |text: &str, scalar| Expr::Cast {
            expr: Box::new(Expr::Literal(Value::Utf8(text.into()))),
            target: CastTarget::Fixed(LogicalType::Scalar(scalar)),
        };
        let floor = |expr| Expr::Call { name: "floor".into(), args: vec![expr], return_type: LogicalType::Scalar(ScalarType::F64).maybe() };
        let nan = f64::from_bits(0x7ff8_0000_0000_0042);
        let doubles = case(vec![
            (0, floor(cast("-0.0", ScalarType::F64))),
            (1, Expr::Literal(Value::F64(nan))),
            (1, cast("unreached double", ScalarType::F64)),
        ], Expr::Literal(Value::Null));
        let floored = floor(case(vec![
            (0, Expr::Literal(Value::F64(-1.25))),
            (1, Expr::Literal(Value::F64(-0.0))),
            (1, cast("unreached floor argument", ScalarType::F64)),
        ], Expr::Literal(Value::Null)));
        let double_type = LogicalType::Scalar(ScalarType::F64).maybe();
        for expression in [&doubles, &floored] {
            assert!(matches!(ProjectionValues::new(expression, &double_type, len), ProjectionValues::F64(_)));
        }
        let truth = Expr::Unary { op: UnaryOp::IsTrue, expr: Box::new(case(vec![(0, Expr::Literal(Value::Bool(true)))], Expr::Literal(Value::Null))) };
        let bool_type = LogicalType::Scalar(ScalarType::Bool);
        assert!(matches!(ProjectionValues::new(&truth, &bool_type, len), ProjectionValues::Bool(_)));
        let single_nan = f32::from_bits(0x7fc0_0042);
        let singles = case(vec![
            (0, cast("-0.0", ScalarType::F32)),
            (1, Expr::Literal(Value::F32(single_nan))),
            (1, cast("unreached real", ScalarType::F32)),
        ], Expr::Literal(Value::Null));
        let single_nulls = case(vec![(3, cast("unreached real", ScalarType::F32))], Expr::Literal(Value::Null));
        let single_type = LogicalType::Scalar(ScalarType::F32).maybe();
        assert!(matches!(ProjectionValues::new(&singles, &single_type, len), ProjectionValues::F32(_)));
        let integers = Expr::Case {
            branches: vec![(
                Expr::binary(BinaryOp::Eq, selector.clone(), Expr::Literal(Value::I64(0))),
                Expr::Literal(Value::I64(i64::MIN)),
            )],
            otherwise: Box::new(cast("9223372036854775807", ScalarType::I64)),
        };
        let nulls = case(vec![(3, cast("unreached integer", ScalarType::I64))], Expr::Literal(Value::Null));
        let mixed = case(vec![(0, Expr::Literal(Value::I64(5)))], Expr::Literal(Value::F64(0.5)));

        let mut cursor = BatchExpressions::new(input(len), schema.clone(), vec![doubles, floored, truth, singles, single_nulls, integers, nulls, mixed], execution.clone());
        let page = cursor.next_batch().await.unwrap().unwrap();
        let [
            Column::Arrow { array: doubles, field: double_field, logical: Some(double) },
            Column::Arrow { array: floored, logical: Some(floored_type), .. },
            Column::Arrow { array: truths, logical: Some(truth_type), .. },
            Column::Arrow { array: singles, field: single_field, logical: Some(single) },
            Column::Arrow { array: single_nulls, logical: Some(LogicalType::Null), .. },
            Column::Arrow { array: integers, field: integer_field, logical: Some(integer) },
            Column::Arrow { array: nulls, logical: Some(LogicalType::Null), .. },
            mixed,
        ] = page.columns.as_slice() else { panic!("Expected Arrow columns with observed nullability"); };
        assert_eq!(*double, LogicalType::Scalar(ScalarType::F64).maybe());
        assert_eq!(*floored_type, double_type);
        assert_eq!(*truth_type, bool_type);
        assert!(double_field.is_nullable());
        assert_eq!(*single, single_type);
        assert!(single_field.is_nullable());
        assert_eq!(single_nulls.data_type(), &DataType::Null);
        assert_eq!(single_nulls.len(), len);
        assert_eq!(*integer, LogicalType::Scalar(ScalarType::I64));
        assert!(!integer_field.is_nullable());
        assert_eq!(nulls.data_type(), &DataType::Null);
        assert_eq!(nulls.len(), len);
        let doubles = doubles.as_any().downcast_ref::<Float64Array>().unwrap();
        let floored = floored.as_any().downcast_ref::<Float64Array>().unwrap();
        let truths = truths.as_any().downcast_ref::<BooleanArray>().unwrap();
        let singles = singles.as_any().downcast_ref::<Float32Array>().unwrap();
        let integers = integers.as_any().downcast_ref::<Int64Array>().unwrap();
        for (row, floor) in floored.iter().enumerate() {
            assert_eq!(floor.map(f64::to_bits), match row % 3 {
                0 => Some((-2.0_f64).to_bits()),
                1 => Some((-0.0_f64).to_bits()),
                _ => None,
            });
            assert_eq!(truths.value(row), row % 3 == 0);
            match row % 3 {
                0 => {
                    assert!(!doubles.is_null(row));
                    assert_eq!(doubles.value(row).to_bits(), (-0.0f64).to_bits());
                    assert!(!singles.is_null(row));
                    assert_eq!(singles.value(row).to_bits(), (-0.0f32).to_bits());
                    assert_eq!(integers.value(row), i64::MIN);
                }
                1 => {
                    assert!(!doubles.is_null(row));
                    assert_eq!(doubles.value(row).to_bits(), nan.to_bits());
                    assert!(!singles.is_null(row));
                    assert_eq!(singles.value(row).to_bits(), single_nan.to_bits());
                    assert_eq!(integers.value(row), i64::MAX);
                }
                _ => {
                    assert!(doubles.is_null(row));
                    assert!(singles.is_null(row));
                    assert_eq!(integers.value(row), i64::MAX);
                }
            }
        }
        assert_eq!(mixed.value(0).unwrap(), Value::I64(5));
        assert_eq!(mixed.value(1).unwrap(), Value::F64(0.5));
        drop(page);
        drop(cursor);
        assert_eq!(memory.used_bytes(), baseline);

        for (scalar, first, text, second, physical) in [
            (ScalarType::Bool, Value::Bool(false), "true", Value::Bool(true), DataType::Boolean),
            (ScalarType::I8, Value::I8(i8::MIN), "127", Value::I8(i8::MAX), DataType::Int8),
            (ScalarType::I16, Value::I16(i16::MIN), "32767", Value::I16(i16::MAX), DataType::Int16),
            (ScalarType::I32, Value::I32(i32::MIN), "2147483647", Value::I32(i32::MAX), DataType::Int32),
        ] {
            let expression = case(vec![
                (0, Expr::Literal(first.clone())),
                (1, cast(text, scalar.clone())),
                (1, cast("unreached cast", scalar.clone())),
            ], Expr::Literal(Value::Null));
            let ty = LogicalType::Scalar(scalar.clone()).maybe();
            assert!(matches!(
                ProjectionValues::new(&expression, &ty, len),
                ProjectionValues::Bool(_) | ProjectionValues::I8(_) | ProjectionValues::I16(_) | ProjectionValues::I32(_),
            ));
            let nulls = case(vec![(3, cast("unreached cast", scalar))], Expr::Literal(Value::Null));
            let mut cursor = BatchExpressions::new(input(len), schema.clone(), vec![expression, nulls], execution.clone());
            let page = cursor.next_batch().await.unwrap().unwrap();
            let [
                values @ Column::Arrow { array, field, logical: Some(logical) },
                Column::Arrow { array: nulls, logical: Some(LogicalType::Null), .. },
            ] = page.columns.as_slice() else { panic!("Expected a fixed-width column and a null column"); };
            assert_eq!(*logical, ty);
            assert_eq!(array.data_type(), &physical);
            assert!(field.is_nullable());
            assert_eq!(nulls.data_type(), &DataType::Null);
            assert_eq!(nulls.len(), len);
            for row in 0..len {
                let expected = match row % 3 { 0 => &first, 1 => &second, _ => &Value::Null };
                assert_eq!(&values.value(row).unwrap(), expected);
            }
        }
        assert_eq!(memory.used_bytes(), baseline);

        let mismatched = case(vec![(0, Expr::Literal(Value::Bool(true)))], Expr::Literal(Value::I32(1)));
        assert!(matches!(
            ProjectionValues::new(&mismatched, &LogicalType::Scalar(ScalarType::Bool), len),
            ProjectionValues::Dynamic(_),
        ));

        for (scalar, zero) in [
            (ScalarType::I32, Value::I32(0)),
            (ScalarType::I64, Value::I64(0)),
            (ScalarType::F32, Value::F32(0.0)),
        ] {
            let later_row = case(vec![
                (1, cast("first projection, second row", scalar.clone())),
                (2, cast("first projection, third row", scalar)),
            ], Expr::Literal(zero));
            let later_column = floor(case(vec![(0, cast("second projection, first row", ScalarType::I64))], Expr::Literal(Value::I64(0))));
            let projections = vec![later_row, later_column];
            let mut cursor = BatchExpressions::new(input(0), schema.clone(), projections.clone(), execution.clone());
            assert_eq!(cursor.next_batch().await.unwrap().unwrap().len, 0);
            drop(cursor);

            let mut cursor = BatchExpressions::new(input(len), schema.clone(), projections, execution.clone());
            let error = cursor.next_batch().await.err().expect("The first projection fails");
            assert!(error.message.as_deref().expect("The cast error has a message").contains("first projection, second row"));
        }
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
        let selection = predicate_selection(&batch, &predicate, &schema, &execution).await.unwrap();
        assert!(!selection.contains(0));
        assert!(selection.contains(1));

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
        let selection = predicate_selection(&batch, &predicate, &schema, &execution).await.unwrap();
        assert!(selection.contains(0));
    }

    struct RecordProjectionInput(Option<ColumnBatch>);

    #[async_trait::async_trait]
    impl BatchCursor for RecordProjectionInput {
        async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> { Ok(self.0.take()) }
    }

    fn record_projection_cursor(
        batch: ColumnBatch, schema: Schema, expressions: Vec<Expr>, memory: &Arc<MemoryBudget>,
    ) -> BatchExpressions {
        let database = crate::DatabaseSnapshot {
            partitions: vec![],
            storage: Arc::new(rtdb_storage::cache::BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
        };
        let catalog = crate::CatalogSnapshot::new("xtdb", [("xtdb".into(), database)].into(), false).unwrap();
        let context = crate::QueryContext::new(catalog, Arc::clone(memory), Timestamp(0), "UTC", Default::default(),
            &tokio_util::sync::CancellationToken::new()).unwrap();
        let execution = crate::context::Execution {
            context, parameters: Arc::new(crate::parameters::BoundParameters::infer(Vec::new(), memory).unwrap()),
            outer: crate::context::OuterRows::empty(memory).unwrap(), bindings: None, analysis: None,
        };
        BatchExpressions::new(Box::new(RecordProjectionInput(Some(batch))), schema, expressions, execution)
    }

    #[tokio::test]
    async fn record_projection_computed_struct_keeps_child_backing_and_duplicate_order() {
        let source = MemoryBudget::new(1 << 23);
        let memory = MemoryBudget::new(1 << 17);
        let schema = Schema::new(vec![field(0, ScalarType::Utf8), field(1, ScalarType::I64)]).unwrap();
        let payload = "x".repeat(1 << 21);
        let text: ArrayRef = Arc::new(StringArray::from(vec![payload.as_str(), "small"]));
        let inputs = [Arc::clone(&text), Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef];
        let columns = schema.fields().iter().zip(inputs).map(|(field, array)| Column::Arrow {
            array, field: Arc::new(rtdb_columnar::to_field(&field.name, &field.data_type).unwrap()),
            logical: Some(field.data_type.clone()),
        }).collect();
        let batch = ColumnBatch::settled(columns, 2, source.reserve(0).unwrap()).unwrap();
        let owner = Arc::downgrade(batch.column_reservation(0));
        let plus = Expr::binary(BinaryOp::Add, schema.fields()[1].expression(), Expr::Literal(Value::I64(1)));
        let expression = Expr::Struct(vec![
            ("score".into(), Expr::Literal(Value::Null)),
            ("payload".into(), schema.fields()[0].expression()),
            ("score".into(), Expr::Struct(vec![("value".into(), plus)])),
            ("explicit_null".into(), Expr::Literal(Value::Null)),
        ]);
        let score = Expr::Field {
            data_type: rtdb_plan::field_type(&expression.data_type(), "score"),
            expr: Box::new(expression.clone()), name: "score".into(),
        };
        let score = Expr::Field {
            data_type: rtdb_plan::field_type(&score.data_type(), "value"),
            expr: Box::new(score), name: "value".into(),
        };
        let mut cursor = record_projection_cursor(batch, schema, vec![expression, score], &memory);
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert!(owner.upgrade().is_some());
        let [Column::Arrow { array, .. }, Column::Arrow { array: score, .. }] = page.columns.as_slice() else { panic!("Expected an Arrow struct and its projected field"); };
        assert_eq!(score.as_any().downcast_ref::<Int64Array>().unwrap().values().as_ref(), &[11, 21]);
        let record = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(record.fields().iter().map(|field| field.name().as_str()).collect::<Vec<_>>(), ["score", "payload", "explicit_null"]);
        assert!(Arc::ptr_eq(record.column(1), &text));
        let scores = record.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(scores.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().as_ref(), &[11, 21]);
        assert_eq!(record.column(2).logical_null_count(), 2);
        drop(page);
        assert!(owner.upgrade().is_none());
        assert_eq!(source.used_bytes(), 0);
    }

    #[tokio::test]
    async fn record_projection_reaches_errors_in_row_then_field_order() {
        let memory = MemoryBudget::new(1 << 21);
        let schema = Schema::new(vec![field(0, ScalarType::I64)]).unwrap();
        let batch = || {
            let input = arrow(Arc::new(Int64Array::from(vec![1, i64::MAX])), ScalarType::I64);
            ColumnBatch::settled(vec![input], 2, memory.reserve(0).unwrap()).unwrap()
        };
        let plus = Expr::binary(BinaryOp::Add, schema.fields()[0].expression(), Expr::Literal(Value::I64(1)));
        let first_row_error = Expr::Cast {
            expr: Box::new(Expr::Literal(Value::Utf8("first row error".into()))),
            target: rtdb_plan::CastTarget::Fixed(LogicalType::Scalar(ScalarType::I64)),
        };
        let expression = Expr::Struct(vec![("duplicate".into(), plus.clone()), ("duplicate".into(), first_row_error)]);
        let mut cursor = record_projection_cursor(batch(), schema.clone(), vec![expression], &memory);
        let error = cursor.next_batch().await.err().expect("The first row fails");
        assert!(error.message.unwrap().contains("first row error"));
        drop(cursor);

        let expression = Expr::Struct(vec![("duplicate".into(), plus), ("duplicate".into(), Expr::Literal(Value::Null))]);
        let mut cursor = record_projection_cursor(batch(), schema, vec![expression], &memory);
        let error = cursor.next_batch().await.err().expect("The overwritten field still runs");
        assert_eq!(error.code, "xtdb.expression/overflow-error");
    }

    #[tokio::test]
    async fn record_projection_reads_nested_nullable_selected_parents() {
        use rtdb_types::StructFields;

        let source = MemoryBudget::new(1 << 20);
        let memory = MemoryBudget::new(1 << 17);
        let integer = LogicalType::Scalar(ScalarType::I64);
        let inner = LogicalType::Struct(StructFields::from([("leaf".into(), integer.clone())])).maybe();
        let outer = LogicalType::Struct(StructFields::from([("inner".into(), inner.clone()), ("score".into(), integer.clone())])).maybe();
        let mut input = field(0, ScalarType::I64);
        input.data_type = outer.clone();
        let schema = Schema::new(vec![input.clone()]).unwrap();
        let doc = |value| Value::Struct(StructFields::from([("inner".into(), value), ("score".into(), Value::I64(10))]));
        let array = rtdb_columnar::copy_array(&outer, &[
            doc(Value::Struct(StructFields::from([("leaf".into(), Value::I64(42))]))),
            Value::Null,
            doc(Value::Null),
        ]).unwrap();
        let field = Arc::new(rtdb_columnar::to_field(&input.name, &outer).unwrap());
        let owner = source.reserve(array.get_array_memory_size()).unwrap();
        let weak = Arc::downgrade(&owner);
        let picks = Arc::new(vec![(0, 1), (0, 0), (0, 2), (1, 0), (0, 0)]);
        let selected = SelectedColumn::new(vec![Some((array, field, Some(outer), Some(owner))), None], picks);
        let batch = ColumnBatch::settled(vec![Column::Selected(selected)], 5, memory.reserve(0).unwrap()).unwrap();
        let leaf = Expr::Field {
            expr: Box::new(Expr::Field { expr: Box::new(input.expression()), name: "inner".into(), data_type: inner }),
            name: "leaf".into(), data_type: integer.clone().maybe(),
        };
        let score = Expr::Field { expr: Box::new(input.expression()), name: "score".into(), data_type: integer.maybe() };
        let plus = Expr::binary(BinaryOp::Add, score, Expr::Literal(Value::I64(1)));
        let kernel = functions::column(&plus, &schema, &batch, rtdb_expr::Parameters::Values(&[]), &memory).unwrap().unwrap();
        assert_eq!(kernel.value(0).unwrap(), Value::Null);
        assert_eq!(kernel.value(1).unwrap(), Value::I64(11));
        assert_eq!(kernel.value(3).unwrap(), Value::Null);

        let mut cursor = record_projection_cursor(batch, schema, vec![leaf, plus], &memory);
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert!(matches!(page.columns[0], Column::Selected(_)));
        assert!(weak.upgrade().is_some());
        for (row, expected) in [Value::Null, Value::I64(42), Value::Null, Value::Null, Value::I64(42)].into_iter().enumerate() {
            assert_eq!(page.columns[0].value(row).unwrap(), expected);
        }
        drop(page);
        assert!(weak.upgrade().is_none());
        assert_eq!(source.used_bytes(), 0);
    }

    #[tokio::test]
    async fn record_projection_row_copy_keeps_selected_nested_layout_and_last_field() {
        use rtdb_types::StructFields;

        let memory = MemoryBudget::new(1 << 20);
        let mut documents = field(0, ScalarType::I64);
        let mut discarded = field(1, ScalarType::I64);
        discarded.name = documents.name.clone();
        let record = |z| Value::Struct(StructFields::from([("z".into(), Value::I64(z)), ("a".into(), Value::Null)]));
        let first = record(1);
        let second = record(2);
        documents.data_type = first.logical_type();
        let physical = Arc::new(rtdb_columnar::to_field("documents", &documents.data_type).unwrap());
        let array = rtdb_columnar::copy_array(&documents.data_type, &[first.clone(), second.clone()]).unwrap();
        let charge = memory.reserve(array.get_array_memory_size()).unwrap();
        let selection = SelectedColumn::new(
            vec![Some((array, physical, Some(documents.data_type.clone()), Some(charge))), None],
            Arc::new(vec![(0, 1), (1, 0), (0, 0), (0, 1)]),
        );
        let raw = Int64Array::from(vec![3, 4, 5, 6]);
        let raw_type = discarded.data_type.clone();
        let batch = ColumnBatch::settled(vec![
            Column::Selected(selection),
            Column::Arrow { array: Arc::new(raw), field: Arc::new(rtdb_columnar::to_field("discarded", &raw_type).unwrap()), logical: Some(raw_type) },
        ], 4, memory.reserve(0).unwrap()).unwrap();
        let schema = Schema::new(vec![documents.clone(), field(1, ScalarType::I64)]).unwrap();
        let expression = Expr::RowCopy(vec![discarded, documents.clone()]);
        let mut cursor = record_projection_cursor(batch, schema, vec![expression], &memory);
        let page = cursor.next_batch().await.unwrap().unwrap();
        let [Column::Arrow { array, .. }] = page.columns.as_slice() else { panic!("Expected an Arrow row copy"); };
        let array = array.as_any().downcast_ref::<StructArray>().unwrap();
        let nested = array.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(nested.fields().iter().map(|field| field.name().as_str()).collect::<Vec<_>>(), ["z", "a"]);
        for (row, expected) in [second.clone(), Value::Null, first, second].into_iter().enumerate() {
            assert_eq!(page.columns[0].value(row).unwrap(), Value::Struct(StructFields::from([(documents.name.clone(), expected)])));
        }
    }

    #[tokio::test]
    async fn record_projection_row_copy_distinguishes_absent_and_null_nested_keys() {
        use rtdb_types::StructFields;

        let memory = MemoryBudget::new(1 << 20);
        let absent = Value::Struct(StructFields::from([("id".into(), Value::I64(1))]));
        let explicit = Value::Struct(StructFields::from([("id".into(), Value::I64(2)), ("optional".into(), Value::Null)]));
        let mut documents = field(0, ScalarType::I64);
        documents.data_type = absent.logical_type().join(&explicit.logical_type());
        let sources = [&absent, &explicit].into_iter().map(|value| {
            let logical = value.logical_type();
            let field = Arc::new(rtdb_columnar::to_field("documents", &logical).unwrap());
            let array = rtdb_columnar::copy_array(&logical, std::slice::from_ref(value)).unwrap();
            let owner = memory.reserve(array.get_array_memory_size()).unwrap();
            Some((array, field, Some(logical), Some(owner)))
        }).collect();
        let selection = SelectedColumn::new(sources, Arc::new(vec![(1, 0), (0, 0)]));
        let batch = ColumnBatch::settled(vec![Column::Selected(selection)], 2, memory.reserve(0).unwrap()).unwrap();
        let schema = Schema::new(vec![documents.clone()]).unwrap();
        let mut cursor = record_projection_cursor(batch, schema, vec![Expr::RowCopy(vec![documents.clone()])], &memory);
        let page = cursor.next_batch().await.unwrap().unwrap();
        for (row, expected) in [explicit, absent].into_iter().enumerate() {
            assert_eq!(page.columns[0].value(row).unwrap(), Value::Struct(StructFields::from([(documents.name.clone(), expected)])));
        }
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
    fn the_arithmetic_kernels_agree_with_the_evaluator_on_every_batch_they_compute() {
        let memory = MemoryBudget::new(1 << 28);
        let schema = Schema::new(vec![field(0, ScalarType::I64), field(1, ScalarType::F64), field(2, ScalarType::Utf8), field(3, ScalarType::Bool), field(4, ScalarType::I64)]).unwrap();
        let mut random = Random(0x2545f4914f6cdd1d);
        let (mut computed, mut raised) = (0, 0);
        for _ in 0..600 {
            let batch = batch(&mut random, 64, &memory);
            let expression = arithmetic(&mut random, &schema, 3);
            let scope = rtdb_expr::EvaluationScope::new(&memory).unwrap();
            let context = rtdb_expr::EvalContext { parameters: rtdb_expr::Parameters::Values(&[]), parameter_types: None, current_time: rtdb_types::Instant::from(Timestamp(0)), timezone: "UTC",
                outer_rows: &[], snapshot_token: None, allocation: Some(&scope), catalog: None };
            let rows: Vec<Vec<Value>> = (0..batch.len).map(|row| batch.columns.iter().map(|column| column.value(row).unwrap()).collect()).collect();
            let column = match functions::column(&expression, &schema, &batch, rtdb_expr::Parameters::Values(&[]), &memory) {
                Ok(Some(column)) => column,
                Ok(None) => continue,
                Err(error) => {
                    // An overflowing cell raises the evaluator's error for it rather than leaving the batch to it.
                    let failed = rows.iter().any(|values| rtdb_expr::evaluate_projection(&expression, &schema, values, &context).is_err_and(|failure| failure.code == error.code));
                    assert!(failed, "{expression:?}: the kernel raised {error:?} where the evaluator doesn't");
                    raised += 1;
                    continue;
                }
            };
            computed += 1;
            for (row, values) in rows.iter().enumerate() {
                let expected = rtdb_expr::evaluate_projection(&expression, &schema, values, &context).unwrap();
                assert_eq!(column.value(row).unwrap(), expected, "{expression:?} on {values:?}");
            }
        }
        assert!(computed > 60 && raised > 0, "the kernels should compute many generated sums: computed {computed}, raised {raised}");
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
            assert_eq!(functions::truth(&predicate, &schema, &batch, rtdb_expr::Parameters::Values(&[])).unwrap(), Some(expected.map(Some).to_vec()), "{predicate:?}");
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
            let truth = functions::truth(&predicate, &schema, &batch, rtdb_expr::Parameters::Values(&parameters)).unwrap().unwrap_or_else(|| panic!("the kernel evaluates {predicate:?}"));
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
            let Some(truth) = functions::truth(&predicate, &schema, &batch, rtdb_expr::Parameters::Values(&[])).unwrap() else { continue };
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
                    let Some(truth) = functions::truth(&predicate, &schema, &batch, rtdb_expr::Parameters::Values(&[])).unwrap() else { continue };
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
                    // As `BatchExpressions` tries them: the copying projections, then the native evaluator.
                    let column = match projection::bound(&expression, &schema, &batch).unwrap() {
                        Some(_) => projection::column(&expression, &schema, &batch, rtdb_expr::Parameters::Values(&[])).unwrap(),
                        None => functions::column(&expression, &schema, &batch, rtdb_expr::Parameters::Values(&[]), &memory).unwrap(),
                    };
                    let Some(column) = column else { continue };
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
pub(crate) struct BatchProject { input: BoxBatchCursor, positions: Arc<[usize]>, memory: Arc<MemoryBudget> }
impl BatchProject {
    pub fn new(input: BoxBatchCursor, positions: Vec<usize>, memory: Arc<MemoryBudget>) -> Self { Self::shared(input, positions.into(), memory) }
    pub(crate) fn shared(input: BoxBatchCursor, positions: Arc<[usize]>, memory: Arc<MemoryBudget>) -> Self { Self { input, positions, memory } }
}
#[async_trait::async_trait]
impl BatchCursor for BatchProject {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        tokio::task::consume_budget().await;
        let Some(mut batch) = poll_nested_batch(self.input.as_mut()).await? else { return Ok(None) };
        let width = self.positions.len();
        if self.positions.iter().copied().eq(0..batch.columns.len()) { return Ok(Some(batch)); }
        let reservation = self.memory.reserve(projection_page_bytes(width)?)?;
        let mut output = ColumnBatch { columns: Vec::with_capacity(width), len: batch.len, reservation, appended: Vec::with_capacity(width) };
        if width == 0 { return Ok(Some(output)); }

        let scratch = batch.columns.len().checked_mul(size_of::<usize>()).ok_or_else(size_error)?;
        let _uses = self.memory.reserve(scratch)?;
        let mut uses = vec![0usize; batch.columns.len()];
        for &position in self.positions.iter() { uses[position] += 1; }
        for &position in self.positions.iter() {
            uses[position] -= 1;
            let owner = batch.column_reservation(position);
            let (column, owner) = if uses[position] == 0 {
                let owner = Arc::clone(owner);
                let column = std::mem::replace(&mut batch.columns[position], Column::Values(Vec::new()));
                (column, owner)
            } else {
                batch.columns[position].duplicate_for_projection(owner, &self.memory)?
            };
            output.columns.push(column);
            output.appended.push(owner);
        }
        Ok(Some(output))
    }
    fn skip_complete_pages(&mut self, rows: u64) -> Result<u64> { self.input.skip_complete_pages(rows) }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { self.input.trace_attributes(span) }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
}

fn projection_page_bytes(width: usize) -> Result<usize> {
    width.checked_mul(size_of::<Column>() + size_of::<Arc<MemoryReservation>>())
        .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)
}

impl Column {
    fn duplicate(&self) -> Self {
        match self {
            Self::Arrow { array, field, logical } => Self::Arrow { array: Arc::clone(array), field: Arc::clone(field), logical: logical.clone() },
            Self::Selected(column) => Self::Selected(column.clone()),
            Self::Values(values) => Self::Values(values.clone()),
        }
    }

    fn duplicate_for_projection(&self, owner: &Arc<MemoryReservation>, memory: &Arc<MemoryBudget>)
        -> Result<(Self, Arc<MemoryReservation>)> {
        let reservation = match self {
            Self::Arrow { logical, .. } => {
                let copied = logical.as_ref().map_or(Ok(0), |logical| {
                    logical.retained_bytes()?.checked_sub(size_of::<LogicalType>()).ok_or_else(size_error)
                })?;
                if copied == 0 { Arc::clone(owner) }
                else { memory.reserve_with_backing(copied, std::iter::once(Arc::clone(owner)))? }
            }
            Self::Selected(_) => Arc::clone(owner),
            Self::Values(values) => {
                let (copied, shared) = values.iter().try_fold((0usize, false), |(bytes, shared), value| {
                    let backing = ColumnBatch::replay_value_backing(value)?;
                    let copied = value.retained_bytes()?.checked_sub(backing).ok_or_else(size_error)?;
                    Ok::<_, rtdb_types::Anomaly>((bytes.checked_add(copied).ok_or_else(size_error)?, shared || backing != 0))
                })?;
                if shared { memory.reserve_with_backing(copied, std::iter::once(Arc::clone(owner)))? }
                else { memory.reserve(copied)? }
            }
        };
        Ok((self.duplicate(), reservation))
    }
}

/// Projects batches through expressions computed over whole columns where their plan-time binding against the schema
/// allows, and row by row through the evaluator otherwise.
pub(crate) struct BatchExpressions {
    input: BoxBatchCursor, schema: Arc<rtdb_plan::Schema>, expressions: Arc<[rtdb_plan::Expr]>, clocked: Option<Arc<[rtdb_plan::Expr]>>,
    cosine: Option<Vec<Option<crate::cosine::Projection>>>,
    functions: Arc<[Option<functions::Projection>]>, functions_clocked: bool,
    admissions: Arc<[crate::compiled::Admission]>, execution: crate::context::Execution, validate_input: bool,
    scalar_fallbacks: Option<u64>,
}
impl BatchExpressions {
    pub fn new(input: BoxBatchCursor, schema: rtdb_plan::Schema, expressions: Vec<rtdb_plan::Expr>, execution: crate::context::Execution) -> Self {
        let expressions: Arc<[rtdb_plan::Expr]> = expressions.into();
        let has_clock = expressions.iter().any(projection::has_clock);
        let functions = expressions.iter().map(|expr| {
            (!projection::has_clock(expr)).then(|| functions::Projection::bind(expr, &schema)).flatten()
        }).collect();
        let admissions = expressions.iter().map(crate::compiled::Admission::of).collect();
        let clocked = (!has_clock).then(|| Arc::clone(&expressions));
        let scalar_fallbacks = operator_evidence_enabled(&execution.context).then_some(0);
        Self { input, schema: Arc::new(schema), expressions, clocked, cosine: None, functions, functions_clocked: !has_clock, admissions, execution, validate_input: false, scalar_fallbacks }
    }
    /// Computes a compiled plan's projections, sharing their plan-time bindings and admissions with every execution of that plan.
    pub(crate) fn admitted(input: BoxBatchCursor, projections: &crate::compiled::Projections, execution: crate::context::Execution) -> Self {
        let clocked = (!projections.has_clock).then(|| Arc::clone(&projections.expressions));
        let scalar_fallbacks = operator_evidence_enabled(&execution.context).then_some(0);
        Self { input, schema: Arc::clone(&projections.schema), expressions: Arc::clone(&projections.expressions), clocked, cosine: None,
            functions: Arc::clone(&projections.functions), functions_clocked: !projections.has_clock,
            admissions: Arc::clone(&projections.admissions), execution, validate_input: false, scalar_fallbacks }
    }
    pub fn validating_input(mut self, validate: bool) -> Self {
        self.validate_input = validate;
        self
    }
}

/// The expressions with each read of the query clock replaced by the instant the evaluator reads, for the kernels;
/// a read the evaluator fails stays a call, which leaves its expression to the evaluator.
async fn read_clock(expressions: &Arc<[rtdb_plan::Expr]>, execution: &crate::context::Execution) -> Result<Arc<[rtdb_plan::Expr]>> {
    let mut reads = Vec::new();
    for expression in expressions.iter() {
        expression.visit(&mut |expr| if let rtdb_plan::Expr::Call { name, args, .. } = expr && projection::clock(name, args) && !reads.contains(expr) { reads.push(expr.clone()); });
    }
    let schema = rtdb_plan::Schema::default();
    let mut instants = Vec::with_capacity(reads.len());
    for read in reads {
        let scope = execution.scope()?;
        if let Ok(value) = execution.evaluate(&scope, &read, &schema, &[]).await { instants.push((read, value)); }
    }
    if instants.is_empty() { return Ok(Arc::clone(expressions)); }
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
        if !self.functions_clocked {
            let clocked = self.clocked.as_deref().unwrap_or_default();
            self.functions = clocked.iter().enumerate().map(|(index, expr)| {
                if projection::has_clock(&self.expressions[index]) {
                    functions::Projection::bind(expr, &self.schema)
                } else {
                    self.functions[index].clone()
                }
            }).collect();
            self.functions_clocked = true;
        }
        let clocked = self.clocked.as_deref().unwrap_or_default();
        if self.cosine.is_none() && batch.len != 0 {
            self.cosine = Some(clocked.iter().map(|expr| {
                crate::cosine::Projection::bind(expr, &self.schema, &self.execution.parameters, &self.execution.context.memory)
            }).collect::<Result<_>>()?);
        }
        let parameters = self.execution.parameters.evaluator();
        let memory = &self.execution.context.memory;
        let width = self.expressions.len();
        let base = projection_page_bytes(width)?;
        let mut reservation = memory.reserve(base)?;
        let _owner_slots = memory.reserve(width.checked_mul(size_of::<Option<Arc<MemoryReservation>>>()).ok_or_else(size_error)?)?;
        let mut owners = vec![None; width];
        let mut columns = Vec::with_capacity(width);
        for (index, (expression, kernel)) in self.expressions.iter().zip(clocked).enumerate() {
            if let Some(projection) = self.cosine.as_ref().and_then(|items| items[index].as_ref()) {
                let bytes = batch.len.checked_mul(9).and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)?;
                if let Ok(mut scratch) = self.execution.context.memory.reserve(bytes)
                    && let Some(column) = projection.column(&batch) {
                    let charge = Arc::get_mut(&mut scratch).expect("Cosine projection owns its reservation");
                    let retained = column.retained_bytes()?;
                    if retained <= charge.bytes() {
                        charge.shrink_to(retained)?;
                    } else {
                        charge.grow_to(retained)?;
                    }
                    let lease = Arc::get_mut(&mut reservation).expect("Batch projection owns its reservation");
                    lease.absorb(charge)?;
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
            if let Some((column, owner)) = projection::coalesce_selection(kernel, &self.schema, &batch, memory)? {
                columns.push(column);
                owners[index] = Some(owner);
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
                && let Some((column, mut charge)) = patch::merge(args, &self.schema, &batch, &self.execution.context.memory)? {
                self.execution.context.check_cancelled()?;
                let lease = Arc::get_mut(&mut reservation).expect("The patch projection owns its reservation");
                lease.absorb(Arc::get_mut(&mut charge).expect("The patch projection owns the merge reservation"))?;
                columns.push(column);
                continue;
            }
            if let Some((column, owner)) = projection::shared_column(kernel, &self.schema, &batch, memory)? {
                validate_kernel_projection(expression, &column)?;
                columns.push(column);
                owners[index] = Some(owner);
                continue;
            }
            if let Some((column, owner)) = projection::computed_record(kernel, &self.schema, &batch, parameters, memory)? {
                columns.push(column);
                owners[index] = Some(owner);
                continue;
            }
            if let Some((column, owner)) = concat::utf8_column(kernel, &self.schema, &batch, &self.execution.context)? {
                columns.push(column);
                owners[index] = Some(owner);
                continue;
            }
            if let Some((column, owner)) = periods::construct(kernel, &self.schema, &batch, &self.execution.context)? {
                columns.push(column);
                owners[index] = Some(owner);
                continue;
            }
            if let Some((column, owner)) = periods::range_bins(kernel, &self.schema, &batch, &self.execution.context)? {
                columns.push(column);
                owners[index] = Some(owner);
                continue;
            }
            if let Some(bound) = projection::bound(kernel, &self.schema, &batch)? {
                let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("column-size", "A column page's charge is shared"))?;
                let retained = lease.bytes();
                let next = lease.bytes().checked_add(bound).ok_or_else(size_error)?;
                match lease.grow_to(next) {
                    Ok(()) => {
                        if let Some(column) = projection::column(kernel, &self.schema, &batch, parameters)? {
                            validate_kernel_projection(expression, &column)?;
                            columns.push(column);
                            continue;
                        }
                        lease.shrink_to(retained)?;
                    }
                    Err(error) if matches!(kernel, rtdb_plan::Expr::List(_)) && error.is(rtdb_types::AnomalyKind::ResourceExhausted) => {},
                    Err(error) => return Err(error),
                }
            }
            if let Some(projection) = self.functions[index].as_ref() {
                let admission = &self.admissions[index];
                if !admission.admitted() {
                    let metadata = self.execution.scope()?;
                    self.execution.prepare_expression(expression, &self.schema, &metadata)?;
                    admission.admit();
                }
                if let Some(column) = projection.column(&batch, parameters, memory)? {
                    let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("column-size", "A column page's charge is shared"))?;
                    lease.grow_to(lease.bytes().checked_add(column.retained_bytes()?).ok_or_else(size_error)?)?;
                    columns.push(column);
                    continue;
                }
            }
            let metadata = self.execution.scope()?;
            let invocation = self.execution.prepare_expression(expression, &self.schema, &metadata)?;
            let scope = self.execution.scope()?;
            let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("column-size", "A column page's charge is shared"))?;
            let retained = lease.bytes();
            let value_slots = batch.len.checked_mul(std::mem::size_of::<Value>()).ok_or_else(size_error)?;
            let primitive_bound = batch.len.checked_mul(9).and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)?;
            if let Some(fallbacks) = &mut self.scalar_fallbacks && batch.len != 0 {
                *fallbacks = fallbacks.saturating_add(1);
            }
            // The bounded score writer must leave at least as much room for row evaluation as the value buffer would.
            if crate::cosine::has_score_output(expression, primitive_bound <= value_slots)
                && let Some(column) = evaluate_score_column(&batch, &self.execution, &invocation, &scope, lease, primitive_bound).await?
            {
                columns.push(column);
                continue;
            }
            lease.grow_to(retained.checked_add(value_slots).ok_or_else(size_error)?)?;
            let mut values = if primitive_bound <= value_slots {
                ProjectionValues::new(expression, invocation.data_type(), batch.len)
            } else {
                ProjectionValues::Dynamic(Vec::with_capacity(batch.len))
            };
            for row in 0..batch.len {
                let input = BatchRow { batch: &batch, row, admit_dates: false };
                let value = self.execution.evaluate_invocation_row(
                    &scope, &invocation, rtdb_expr::ExpressionRow::Borrowed(&input),
                ).await?;
                // Keep the same per-row admission so a tight budget fails at the same row.
                lease.grow_to(lease.bytes().checked_add(value.retained_bytes()?).ok_or_else(size_error)?)?;
                values.push(value)?;
                scope.reset()?;
            }
            let values = match values.finish()? {
                Column::Values(values) => values,
                column => {
                    settle_primitive_column(&column, lease, retained)?;
                    columns.push(column);
                    continue;
                }
            };
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
                    Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => columns.push(Column::Values(values)),
                    Err(error) => return Err(error),
                }
            } else {
                columns.push(Column::Values(values));
            }
        }
        let len = batch.len;
        drop(batch);
        finish_expression_page(columns, owners, len, base, reservation, memory).map(Some)
    }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) {
        self.input.trace_attributes(span);
        if let Some(count) = self.scalar_fallbacks {
            OperatorCounter::ProjectionScalarFallbacks.record(span, count);
        }
    }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
}

fn validate_kernel_projection(expression: &Expr, column: &Column) -> Result<()> {
    if !matches!(expression, Expr::Column { .. } | Expr::RowCopy(_))
        && let Column::Arrow { field, array, .. } = column
    {
        rtdb_columnar::validate_projected_decimals(field, array.as_ref())?;
    }
    Ok(())
}

fn owned_projection_bytes(column: &Column) -> Result<usize> {
    let bytes = column.retained_bytes()?;
    match column {
        Column::Arrow { field, logical, .. } => {
            let logical = logical.as_ref().map_or(Ok(0), LogicalType::retained_bytes)?;
            bytes.checked_add(field.size()).and_then(|bytes| bytes.checked_add(logical)).ok_or_else(size_error)
        }
        Column::Selected(selected) => selected.sources.iter().flatten().try_fold(bytes, |bytes, (_, field, _, owner)| {
            bytes.checked_add(if owner.is_none() { field.size() } else { 0 }).ok_or_else(size_error)
        }),
        Column::Values(_) => Ok(bytes),
    }
}

fn finish_expression_page(
    mut columns: Vec<Column>, owners: Vec<Option<Arc<MemoryReservation>>>, len: usize, base: usize,
    mut reservation: Arc<MemoryReservation>, memory: &Arc<MemoryBudget>,
) -> Result<ColumnBatch> {
    let bytes = columns.iter().zip(&owners).filter(|(_, owner)| owner.is_none()).try_fold(base, |bytes, (column, _)| {
        bytes.checked_add(owned_projection_bytes(column)?).ok_or_else(size_error)
    })?;
    let lease = Arc::get_mut(&mut reservation).expect("Batch projection owns its reservation");
    if bytes <= lease.bytes() { lease.shrink_to(bytes)?; } else { lease.grow_to(bytes)?; }

    let pack = columns.iter().any(|column| matches!(column, Column::Values(values) if !values.is_empty()));
    let mut appended = Vec::with_capacity(columns.len());
    for (column, owner) in columns.iter_mut().zip(owners) {
        let mut owner = match owner {
            Some(owner) => owner,
            None => lease.split_off(owned_projection_bytes(column)?)?,
        };
        if pack && let Column::Values(values) = column {
            let (packed, mut charge) = pack_evaluated_column(values, memory)?;
            let bytes = owned_projection_bytes(&packed)?;
            let lease = Arc::get_mut(&mut charge).expect("A packed projection owns its reservation");
            if bytes <= lease.bytes() { lease.shrink_to(bytes)?; } else { lease.grow_to(bytes)?; }
            *column = packed;
            owner = charge;
        }
        appended.push(owner);
    }
    Ok(ColumnBatch { columns, len, reservation, appended })
}

enum ProjectionValues {
    Dynamic(Vec<Value>),
    Bool(BooleanBuilder),
    I8(Int8Builder),
    I16(Int16Builder),
    I32(Int32Builder),
    I64(Int64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
}

impl ProjectionValues {
    fn new(expression: &Expr, output: &LogicalType, len: usize) -> Self {
        let fixed = plain_scalar(output).filter(|scalar| {
            matches!(
                scalar,
                ScalarType::Bool | ScalarType::I8 | ScalarType::I16 | ScalarType::I32
                    | ScalarType::I64 | ScalarType::F32 | ScalarType::F64
            )
                && fixed_projection_result(expression, scalar)
        });
        match fixed {
            Some(ScalarType::Bool) => Self::Bool(BooleanBuilder::with_capacity(len)),
            Some(ScalarType::I8) => Self::I8(Int8Builder::with_capacity(len)),
            Some(ScalarType::I16) => Self::I16(Int16Builder::with_capacity(len)),
            Some(ScalarType::I32) => Self::I32(Int32Builder::with_capacity(len)),
            Some(ScalarType::I64) => Self::I64(Int64Builder::with_capacity(len)),
            Some(ScalarType::F32) => Self::F32(Float32Builder::with_capacity(len)),
            Some(ScalarType::F64) => Self::F64(Float64Builder::with_capacity(len)),
            _ => Self::Dynamic(Vec::with_capacity(len)),
        }
    }

    fn push(&mut self, value: Value) -> Result<()> {
        match (self, value) {
            (Self::Dynamic(values), value) => values.push(value),
            (Self::Bool(values), Value::Bool(value)) => values.append_value(value),
            (Self::Bool(values), Value::Null) => values.append_null(),
            (Self::I8(values), Value::I8(value)) => values.append_value(value),
            (Self::I8(values), Value::Null) => values.append_null(),
            (Self::I16(values), Value::I16(value)) => values.append_value(value),
            (Self::I16(values), Value::Null) => values.append_null(),
            (Self::I32(values), Value::I32(value)) => values.append_value(value),
            (Self::I32(values), Value::Null) => values.append_null(),
            (Self::I64(values), Value::I64(value)) => values.append_value(value),
            (Self::I64(values), Value::Null) => values.append_null(),
            (Self::F32(values), Value::F32(value)) => values.append_value(value),
            (Self::F32(values), Value::Null) => values.append_null(),
            (Self::F64(values), Value::F64(value)) => values.append_value(value),
            (Self::F64(values), Value::Null) => values.append_null(),
            _ => return Err(rtdb_types::Anomaly::fault("rtdb.query/scalar-result", "Projection returned a value outside its fixed result type")),
        }
        Ok(())
    }

    fn finish(self) -> Result<Column> {
        match self {
            Self::Dynamic(values) => Ok(Column::Values(values)),
            Self::Bool(mut values) => fixed_width_column(values.finish()),
            Self::I8(mut values) => fixed_width_column(values.finish()),
            Self::I16(mut values) => fixed_width_column(values.finish()),
            Self::I32(mut values) => fixed_width_column(values.finish()),
            Self::I64(mut values) => fixed_width_column(values.finish()),
            Self::F32(mut values) => fixed_width_column(values.finish()),
            Self::F64(mut values) => fixed_width_column(values.finish()),
        }
    }
}

fn fixed_projection_result(expression: &Expr, scalar: &ScalarType) -> bool {
    match expression {
        Expr::Case { branches, otherwise } | Expr::SimpleCase { branches, otherwise, .. } => {
            branches.iter().map(|(_, value)| value).chain([otherwise.as_ref()]).all(|value| {
                stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || fixed_projection_result(value, scalar))
            })
        }
        Expr::Call { name, args, .. } if args.len() == 1 && *scalar == ScalarType::F64 => matches!(
            rtdb_expr::kernels::Function::resolve(name),
            Some(rtdb_expr::kernels::Function::Floor | rtdb_expr::kernels::Function::Ceil),
        ),
        Expr::Unary { op: UnaryOp::IsNull | UnaryOp::IsNotNull | UnaryOp::IsUnknown | UnaryOp::IsTrue | UnaryOp::IsFalse, .. } => {
            *scalar == ScalarType::Bool
        }
        Expr::Literal(Value::Null) => true,
        Expr::Literal(Value::Bool(_)) => *scalar == ScalarType::Bool,
        Expr::Literal(Value::I8(_)) => *scalar == ScalarType::I8,
        Expr::Literal(Value::I16(_)) => *scalar == ScalarType::I16,
        Expr::Literal(Value::I32(_)) => *scalar == ScalarType::I32,
        Expr::Literal(Value::I64(_)) => *scalar == ScalarType::I64,
        Expr::Literal(Value::F32(_)) => *scalar == ScalarType::F32,
        Expr::Literal(Value::F64(_)) => *scalar == ScalarType::F64,
        // A cast of an arbitrary expression can forward an undefined input.
        Expr::Cast { expr, target: CastTarget::Fixed(LogicalType::Scalar(target)) } => {
            target == scalar && matches!(expr.as_ref(), Expr::Literal(Value::I64(_) | Value::F64(_) | Value::Utf8(_) | Value::Null))
        }
        _ => false,
    }
}

async fn evaluate_score_column(
    batch: &ColumnBatch, execution: &crate::context::Execution, invocation: &rtdb_expr::ExpressionInvocation<'_>,
    scope: &rtdb_expr::EvaluationScope, lease: &mut MemoryReservation, bound: usize,
) -> Result<Option<Column>> {
    let retained = lease.bytes();
    match lease.grow_to(retained.checked_add(bound).ok_or_else(size_error)?) {
        Ok(()) => (),
        Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => return Ok(None),
        Err(error) => return Err(error),
    }

    let mut scores = Float64Builder::with_capacity(batch.len);
    for row in 0..batch.len {
        let input = BatchRow { batch, row, admit_dates: false };
        match execution.evaluate_invocation_row(scope, invocation, rtdb_expr::ExpressionRow::Borrowed(&input)).await? {
            Value::F64(score) => scores.append_value(score),
            Value::Null => scores.append_null(),
            _ => return Err(rtdb_types::Anomaly::fault("rtdb.query/cosine-result", "Cosine projection produced a value other than a score or null")),
        }
        scope.reset()?;
    }

    let column = fixed_width_column(scores.finish())?;
    settle_primitive_column(&column, lease, retained)?;
    Ok(Some(column))
}

fn fixed_width_column(values: impl Array + 'static) -> Result<Column> {
    let len = values.len();
    let nulls = values.null_count();
    let (array, nullable): (ArrayRef, bool) = if nulls == len {
        drop(values);
        (Arc::new(NullArray::new(len)), true)
    } else {
        (Arc::new(values), nulls != 0)
    };
    let field = Arc::new(ArrowField::new("value", array.data_type().clone(), nullable));
    let logical = rtdb_columnar::from_field(&field)?;
    Ok(Column::Arrow { array, field, logical: Some(logical) })
}

fn settle_primitive_column(column: &Column, lease: &mut MemoryReservation, retained: usize) -> Result<()> {
    let bytes = retained.checked_add(column.retained_bytes()?).ok_or_else(size_error)?;
    if bytes <= lease.bytes() { lease.shrink_to(bytes) } else { lease.grow_to(bytes) }
}

/// Packs scalar results whose Arrow cells read back as the same values and exact execution types.
pub(crate) fn scalar_result(values: &[Value], memory: &Arc<MemoryBudget>) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let Some(ty) = scalar_result_type(values.iter()) else { return Ok(None); };
    match pack_scalar_values(values, ty, memory) {
        Ok(column) => Ok(Some(column)),
        Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => Ok(None),
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
    pack_scalar_cells(values, ty, memory, rtdb_columnar::copy_array)
}

fn pack_scalar_cells<T: Borrow<Value>>(
    values: &[T], ty: LogicalType, memory: &Arc<MemoryBudget>, copy: impl FnOnce(&LogicalType, &[T]) -> Result<ArrayRef>,
) -> Result<(Column, Arc<MemoryReservation>)> {
    let (array, scratch) = if plain_scalar(&ty) == Some(&ScalarType::Utf8) {
        pack_utf8_values(values, memory)?
    } else {
        let scratch = memory.reserve(rtdb_columnar::estimate_builder_bytes_refs(&ty, values.iter().map(Borrow::borrow))?)?;
        (copy(&ty, values)?, scratch)
    };
    let field = Arc::new(rtdb_columnar::to_field("value", &ty)?);
    Ok((Column::Arrow { array, field, logical: Some(ty) }, scratch))
}

fn pack_utf8_values(values: &[impl Borrow<Value>], memory: &Arc<MemoryBudget>) -> Result<(ArrayRef, Arc<MemoryReservation>)> {
    let texts = values.iter().map(|value| match value.borrow() {
        Value::Utf8(text) => Some(text.as_str()),
        Value::Null => None,
        _ => unreachable!("The scalar result type admitted only strings and nulls"),
    });
    let payload = texts.clone().flatten().try_fold(0usize, |bytes, text| {
        bytes.checked_add(text.len()).ok_or_else(size_error)
    })?;
    i32::try_from(payload).map_err(|_| size_error())?;
    // The input values are already charged, and the pre-sized builder copies only their bytes into Arrow.
    let bound = values.len().checked_add(1)
        .and_then(|offsets| offsets.checked_mul(size_of::<i32>()))
        .and_then(|bytes| bytes.checked_add(payload))
        .and_then(|bytes| bytes.checked_add(values.len().div_ceil(8)))
        .and_then(|bytes| bytes.checked_add(4096))
        .ok_or_else(size_error)?;
    let scratch = memory.reserve(bound)?;
    let mut builder = StringBuilder::with_capacity(values.len(), payload);
    for text in texts { builder.append_option(text); }
    Ok((Arc::new(builder.finish()), scratch))
}

pub(crate) fn pack_evaluated_column(values: &[Value], memory: &Arc<MemoryBudget>) -> Result<(Column, Arc<MemoryReservation>)> {
    pack_evaluated_cells(values, memory, rtdb_columnar::copy_array)
}

fn pack_evaluated_cells<T: Borrow<Value>>(
    values: &[T], memory: &Arc<MemoryBudget>, copy: impl Fn(&LogicalType, &[T]) -> Result<ArrayRef>,
) -> Result<(Column, Arc<MemoryReservation>)> {
    let cells_in = values.iter().map(Borrow::borrow);
    if let Some(ty) = scalar_result_type(cells_in.clone()) {
        return pack_scalar_cells(values, ty, memory, copy);
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
    if let Some(kind) = cells_in.clone().find(|value| !value.is_null()).and_then(float_vectors::kind)
        && cells_in.clone().all(|value| value.is_null() || float_vectors::kind(value) == Some(kind.clone()))
        && let Some(packed) = float_vectors::pack(values, kind, memory)?
    {
        return Ok(packed);
    }
    let mut start = 0;
    while start < values.len() {
        if let Some(kind) = float_vectors::kind(values[start].borrow()) {
            let end = start + values[start..].iter().map(Borrow::borrow).take_while(|value| {
                float_vectors::kind(value) == Some(kind.clone())
            }).count();
            if let Some((Column::Arrow { array, field, logical }, custody)) = float_vectors::pack(&values[start..end], kind, memory)? {
                let source = sources.len();
                cells.extend((0..end - start).map(|row| (source, row)));
                sources.push(Some((array, field, logical, Some(custody))));
                start = end;
                continue;
            }
        }
        let ty = cell_type(values[start].borrow());
        let field = Arc::new(rtdb_columnar::to_field("value", &ty)?);
        let mut end = start + 1;
        while end < values.len()
            && rtdb_columnar::to_field("value", &cell_type(values[end].borrow()))? == *field {
            end += 1;
        }
        let run = &values[start..end];
        let bound = rtdb_columnar::estimate_builder_bytes_refs(&ty, run.iter().map(Borrow::borrow))?;
        let mut custody = memory.reserve(bound)?;
        let array = copy(&ty, run)?;
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
    use arrow_array::{Array, ArrayRef, ListArray, StructArray, make_array};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_schema::{DataType, Field as ArrowField};
    use rtdb_plan::{CastTarget, Expr, Schema};
    use rtdb_types::{LogicalType, Result, StructFields, Value};
    use super::{Column, ColumnBatch, MemoryBudget, MemoryReservation, SelectedColumn, StoredVector};

    /// A bare column or parameter of a projection over one bound row, shared rather than computed.
    pub(crate) fn bound_column(expr: &Expr, schema: &Schema, columns: &[Column], parameters: &[Column]) -> Option<Column> {
        match expr {
            Expr::Column { id, .. } => schema.position(*id).map(|position| columns[position].duplicate()),
            Expr::Parameter { index, .. } => parameters.get(*index).map(Column::duplicate),
            _ => None,
        }
    }

    /// Whether an expression has a shape the kernel may compute; its cells' types decide per batch.
    pub(crate) fn shaped(expr: &Expr) -> bool {
        match expr {
            Expr::Column { .. } | Expr::Parameter { .. } | Expr::RowCopy(_) => true,
            Expr::Literal(value) => literal(value),
            // Sums and products the native evaluator computes where their operands' layouts allow.
            Expr::Binary { op: rtdb_plan::BinaryOp::Add | rtdb_plan::BinaryOp::Subtract | rtdb_plan::BinaryOp::Multiply, left, right } => shaped(left) && shaped(right),
            Expr::Struct(fields) => fields.iter().all(|(_, field)| shaped(field)),
            Expr::Field { expr, .. } => matches!(expr.as_ref(), Expr::Column { .. }),
            Expr::Coalesce(args) => args.iter().all(shaped),
            Expr::Call { name, args, .. } if name == "coalesce" => args.iter().all(shaped),
            Expr::Call { name, args, .. } if name == "_patch" => args.iter().all(|arg| matches!(arg, Expr::Column { .. })),
            Expr::Call { name, args, .. } if clock(name, args) => true,
            call if super::search::shaped(call) => true,
            Expr::Cast { expr: inner, target: CastTarget::Fixed(_) } if shaped(inner) => true,
            expr => super::functions::lowerable(expr),
        }
    }
    fn literal(value: &Value) -> bool {
        matches!(value, Value::Null | Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_) | Value::F64(_) | Value::Utf8(_) | Value::Bool(_) | Value::Timestamp { .. })
    }
    /// The query clock, which `BatchExpressions` reads once and hands the kernels as a literal.
    pub(crate) fn clock(name: &str, args: &[Expr]) -> bool { args.is_empty() && matches!(name, "current_timestamp" | "current-timestamp" | "now") }
    /// Whether `expr` contains a query-clock read that `BatchExpressions` substitutes once per execution.
    pub(crate) fn has_clock(expr: &Expr) -> bool {
        let mut found = false;
        expr.visit(&mut |expr| if let Expr::Call { name, args, .. } = expr && clock(name, args) { found = true; });
        found
    }

    pub(super) fn shared_column(expr: &Expr, schema: &Schema, batch: &ColumnBatch, memory: &Arc<MemoryBudget>)
        -> Result<Option<(Column, Arc<MemoryReservation>)>> {
        match expr {
            Expr::Column { id, .. } => {
                let Some(position) = schema.position(*id) else { return Ok(None); };
                let (mut column, owner) = batch.columns[position].duplicate_for_projection(batch.column_reservation(position), memory)?;
                if let Column::Values(values) = &mut column {
                    for value in values { value.admit_expression_dates()?; }
                }
                Ok(Some((column, owner)))
            }
            Expr::Field { .. } => match shared_field_path(expr, schema, batch, memory) {
                Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => Ok(None),
                result => result,
            },
            Expr::Struct(fields) if direct_struct(fields, schema, batch) => {
                let inputs = fields.iter().map(|(name, expr)| {
                    let Expr::Column { id, .. } = expr else { unreachable!("The struct's inputs are columns"); };
                    (name.as_str(), schema.position(*id).expect("The struct's input field was checked"))
                });
                shared_record(inputs, batch, memory)
            }
            Expr::RowCopy(fields) if row_copy_compatible(fields, schema, batch) => {
                let inputs = fields.iter().map(|field| (field.name.as_str(), schema.position(field.id).expect("Row-copy field is in the input")));
                shared_record(inputs, batch, memory)
            }
            Expr::Cast { expr, target: CastTarget::Fixed(target) } if super::plain_scalar(target).is_some_and(super::copyable) => {
                let Some((column, owner)) = shared_column(expr, schema, batch, memory)? else { return Ok(None); };
                Ok(identity_cast(&column, target).then_some((column, owner)))
            }
            _ => Ok(None),
        }
    }

    fn shared_record<'a>(inputs: impl ExactSizeIterator<Item = (&'a str, usize)> + Clone, batch: &ColumnBatch, memory: &Arc<MemoryBudget>)
        -> Result<Option<(Column, Arc<MemoryReservation>)>> {
        assemble_record(inputs.len(), inputs.map(|(name, position)| {
            (name, &batch.columns[position], batch.column_reservation(position))
        }), memory)
    }

    fn assemble_record<'a>(
        width: usize,
        inputs: impl ExactSizeIterator<Item = (&'a str, &'a Column, &'a Arc<MemoryReservation>)> + Clone,
        memory: &Arc<MemoryBudget>,
    ) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
        let bytes = inputs.clone().try_fold(512usize, |bytes, (name, column, _)| {
            let Column::Arrow { field, logical: Some(logical), .. } = column else { unreachable!("The struct's inputs were checked"); };
            let logical = logical.retained_bytes()?;
            field.size().checked_add(logical).and_then(|child| child.checked_add(name.len()))
                .and_then(|child| child.checked_mul(2)).and_then(|child| child.checked_add(512))
                .and_then(|child| bytes.checked_add(child)).ok_or_else(super::size_error)
        })?;
        let owner = memory.reserve_with_backing(bytes, inputs.clone().map(|(_, _, owner)| Arc::clone(owner)))?;
        Ok(record_from_columns(width, inputs.map(|(name, column, _)| (name, column))).map(|column| (column, owner)))
    }

    pub(super) fn computed_record(
        expr: &Expr, schema: &Schema, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>, memory: &Arc<MemoryBudget>,
    ) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
        match try_record(expr, schema, batch, parameters, memory) {
            Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => Ok(None),
            result => result,
        }
    }

    fn try_record(
        expr: &Expr, schema: &Schema, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>, memory: &Arc<MemoryBudget>,
    ) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
        if batch.len == 0 { return Ok(None); }
        let (fields, row_copy) = match expr {
            Expr::Struct(fields) if !fields.is_empty() => (fields.len(), false),
            Expr::RowCopy(fields) if !fields.is_empty() => (fields.len(), true),
            Expr::Field { expr: parent, name, .. } if matches!(parent.as_ref(), Expr::Struct(_) | Expr::Field { .. }) => {
                let Some((parent, owner)) = try_record(parent, schema, batch, parameters, memory)? else { return Ok(None); };
                let result = shared_field(&parent, name, &owner, memory)?;
                return Ok(result.filter(|(column, _)| super::validate_kernel_projection(expr, column).is_ok()));
            }
            _ => return Ok(None),
        };
        let bytes = fields.checked_mul(size_of::<(&str, Column, Arc<MemoryReservation>)>()).ok_or_else(super::size_error)?;
        let _inputs = memory.reserve(bytes)?;
        let mut inputs: Vec<(&str, Column, Arc<MemoryReservation>)> = Vec::with_capacity(fields);
        let mut add = |name, column: Column, owner| {
            let Column::Arrow { field, logical: Some(logical), .. } = &column else { return false; };
            if !record_field(field, true) || !super::stable_read_layout(logical, field) { return false; }
            if let Some(input) = inputs.iter_mut().find(|input| input.0 == name) {
                *input = (name, column, owner);
            } else {
                inputs.push((name, column, owner));
            }
            true
        };
        match expr {
            Expr::Struct(fields) => for (name, expr) in fields {
                let Some((column, owner)) = record_input(expr, schema, batch, parameters, memory)? else { return Ok(None); };
                if !add(name.as_str(), column, owner) { return Ok(None); }
            },
            Expr::RowCopy(fields) => for field in fields {
                let Some(position) = schema.position(field.id) else { return Ok(None); };
                let Some((column, owner)) = record_vector(&batch.columns[position], batch.column_reservation(position), batch.len, memory)? else { return Ok(None); };
                if !add(field.name.as_str(), column, owner) { return Ok(None); }
            },
            _ => unreachable!("The projection is a record"),
        }
        let result = assemble_record(inputs.len(), inputs.iter().map(|(name, column, owner)| (*name, column, owner)), memory)?;
        if !row_copy && let Some((column, _)) = &result && super::validate_kernel_projection(expr, column).is_err() {
            return Ok(None);
        }
        Ok(result)
    }

    fn record_input(
        expr: &Expr, schema: &Schema, batch: &ColumnBatch, parameters: rtdb_expr::Parameters<'_>, memory: &Arc<MemoryBudget>,
    ) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
        match expr {
            Expr::Column { id, .. } => {
                let Some(position) = schema.position(*id) else { return Ok(None); };
                record_vector(&batch.columns[position], batch.column_reservation(position), batch.len, memory)
            }
            Expr::Field { .. } => {
                let Some((column, owner)) = shared_column(expr, schema, batch, memory)? else {
                    return try_record(expr, schema, batch, parameters, memory);
                };
                match column {
                    Column::Arrow { .. } => Ok(Some((column, owner))),
                    _ => record_vector(&column, &owner, batch.len, memory),
                }
            }
            Expr::Struct(_) => try_record(expr, schema, batch, parameters, memory),
            Expr::Cast { expr, target: CastTarget::Fixed(target) } => {
                let Some((column, owner)) = record_input(expr, schema, batch, parameters, memory)? else { return Ok(None); };
                Ok(identity_cast(&column, target).then_some((column, owner)))
            }
            Expr::List(_) | Expr::Literal(_) => {
                let Some(bytes) = bound(expr, schema, batch)? else { return Ok(None); };
                let owner = memory.reserve(bytes)?;
                let Some(column) = column(expr, schema, batch, parameters).ok().flatten() else { return Ok(None); };
                match column {
                    Column::Arrow { .. } => Ok(Some((column, owner))),
                    _ => record_vector(&column, &owner, batch.len, memory),
                }
            }
            Expr::Binary { op: rtdb_plan::BinaryOp::Add | rtdb_plan::BinaryOp::Subtract | rtdb_plan::BinaryOp::Multiply, .. } => {
                // A failed speculative kernel leaves the row evaluator to raise the first reached error across fields.
                let Some(column) = super::functions::column(expr, schema, batch, parameters, memory).ok().flatten() else { return Ok(None); };
                let owner = memory.reserve(super::owned_projection_bytes(&column)?)?;
                Ok(Some((column, owner)))
            }
            _ => Ok(None),
        }
    }

    fn record_vector(
        column: &Column, owner: &Arc<MemoryReservation>, len: usize, memory: &Arc<MemoryBudget>,
    ) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
        match column {
            Column::Arrow { field, logical: Some(logical), .. } if record_field(field, true) && super::stable_read_layout(logical, field) =>
                column.duplicate_for_projection(owner, memory).map(Some),
            Column::Selected(selected) => {
                if let Some(((array, field, Some(logical), custody), rows)) = selected.contiguous_source(0..len)
                    && rows.len() == array.len() && record_field(field, true) && super::stable_read_layout(logical, field)
                {
                    let owner = memory.reserve_with_backing(logical.retained_bytes()?, std::iter::once(Arc::clone(custody.as_ref().unwrap_or(owner))))?;
                    return Ok(Some((Column::Arrow { array: Arc::clone(array), field: Arc::clone(field), logical: Some(logical.clone()) }, owner)));
                }
                if selected.source_layouts().any(|(_, field, logical)| {
                    !logical.is_some_and(|logical| record_field(field, true) && super::stable_read_layout(logical, field))
                }) { return Ok(None); }
                if let Some(((array, field, logical), owner)) = selected.uniform_fragment_vector(0..len, memory).ok().flatten() {
                    return Ok(Some((Column::Arrow { array, field, logical }, owner)));
                }
                let mut layouts = selected.source_layouts().filter_map(|(_, field, logical)| {
                    (field.data_type() != &DataType::Null).then_some((field, logical?))
                });
                let Some((field, logical)) = layouts.next() else { return Ok(None); };
                let nullable = logical.clone().maybe();
                if !layouts.all(|(other, ty)| other.data_type() == field.data_type() && other.metadata() == field.metadata()
                    && ty.clone().maybe() == nullable)
                {
                    return Ok(None);
                }
                let field = Arc::new(field.clone().with_nullable(true));
                Ok(selected.relayout(&nullable, &field, memory, false)?.map(|(array, owner)| {
                    (Column::Arrow { array, field, logical: Some(nullable) }, owner)
                }))
            }
            _ => Ok(None),
        }
    }

    fn record_field(field: &ArrowField, decimals: bool) -> bool {
        match field.data_type() {
            DataType::Null => field.is_nullable(),
            DataType::Struct(fields) => fields.iter().all(|field| record_field(field, decimals)),
            DataType::List(child) | DataType::FixedSizeList(child, _) | DataType::Map(child, _) => record_field(child, decimals),
            DataType::Union(fields, _) => fields.iter().all(|(_, field)| record_field(field, decimals)),
            DataType::RunEndEncoded(..) => false,
            ty => decimals || rtdb_columnar::decimal_parameters(ty).is_none(),
        }
    }

    fn shared_field_path(
        expression: &Expr, schema: &Schema, batch: &ColumnBatch, memory: &Arc<MemoryBudget>,
    ) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
        let Expr::Field { expr, name, .. } = expression else { return Ok(None); };
        if let Expr::Column { id, .. } = expr.as_ref() {
            let Some(position) = schema.position(*id) else { return Ok(None); };
            return shared_field(&batch.columns[position], name, batch.column_reservation(position), memory);
        }
        let Some((parent, owner)) = shared_field_path(expr, schema, batch, memory)? else { return Ok(None); };
        shared_field(&parent, name, &owner, memory)
    }

    fn shared_field(
        column: &Column, name: &str, owner: &Arc<MemoryReservation>, memory: &Arc<MemoryBudget>,
    ) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
        match column {
            Column::Arrow { array, field, logical: Some(logical) } => {
                let Some((parent, child, field, logical)) = struct_child(array, field, logical, name) else { return Ok(None); };
                funded_child(parent, child, field, logical, owner, memory)
            }
            Column::Selected(selected) => {
                let bytes = selected.sources.len().checked_mul(size_of::<Option<StoredVector>>() + 1)
                    .and_then(|bytes| bytes.checked_add(512)).ok_or_else(super::size_error)?;
                let selection_owner = memory.reserve_with_backing(bytes, std::iter::once(Arc::clone(owner)))?;
                let mut used = vec![false; selected.sources.len()];
                for (source, _) in selected.cells.iter() { used[source] = true; }
                let mut sources = Vec::with_capacity(selected.sources.len());
                for (source, used) in selected.sources.iter().zip(used) {
                    let Some((array, field, logical, custody)) = source.as_ref().filter(|_| used) else {
                        sources.push(None);
                        continue;
                    };
                    let Some(logical) = logical else { return Ok(None); };
                    if *logical == LogicalType::Null && field.data_type() == &DataType::Null && field.is_nullable() {
                        sources.push(None);
                        continue;
                    }
                    let Some((parent, child, field, logical)) = struct_child(array, field, logical, name) else { return Ok(None); };
                    if !record_field(field, false) { return Ok(None); }
                    let Some((Column::Arrow { array, field, logical }, charge)) =
                        funded_child(parent, child, field, logical, custody.as_ref().unwrap_or(owner), memory)? else { return Ok(None); };
                    sources.push(Some((array, field, logical, Some(charge))));
                }
                let column = Column::Selected(SelectedColumn { sources: Arc::new(sources), cells: selected.cells.clone() });
                Ok(Some((column, selection_owner)))
            }
            _ => Ok(None),
        }
    }

    fn funded_child(
        parent: &StructArray, child: &ArrayRef, field: &Arc<ArrowField>, logical: &LogicalType,
        source: &Arc<MemoryReservation>, memory: &Arc<MemoryBudget>,
    ) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
        let wrap = parent.null_count() != 0 && child.data_type() != &DataType::Null;
        if wrap && matches!(child.data_type(), DataType::Union(..) | DataType::RunEndEncoded(..)) { return Ok(None); }
        let logical_bytes = logical.retained_bytes()?;
        let owner = if wrap {
            let bitmap = if child.nulls().is_some() { child.len().div_ceil(8) } else { 0 };
            let descriptor = child.get_array_memory_size().checked_sub(child.get_buffer_memory_size()).ok_or_else(super::size_error)?;
            let bytes = logical_bytes.checked_add(field.size()).and_then(|bytes| bytes.checked_add(descriptor))
                .and_then(|bytes| bytes.checked_mul(4)).and_then(|bytes| bytes.checked_add(bitmap))
                .and_then(|bytes| bytes.checked_add(512)).ok_or_else(super::size_error)?;
            memory.reserve_with_backing(bytes, std::iter::once(Arc::clone(source)))?
        } else {
            let copied = logical_bytes.checked_sub(size_of::<LogicalType>()).ok_or_else(super::size_error)?;
            if copied == 0 { Arc::clone(source) }
            else { memory.reserve_with_backing(copied, std::iter::once(Arc::clone(source)))? }
        };
        Ok(project_struct_child(parent, child, field, logical).map(|column| (column, owner)))
    }

    /// Bounds what `column` allocates for `expr`, or `None` where it would not compute it.
    pub(crate) fn bound(expr: &Expr, schema: &Schema, batch: &ColumnBatch) -> Result<Option<usize>> {
        let rows = batch.len;
        let per_row = |bytes: usize| rows.checked_mul(bytes).and_then(|total| total.checked_add(256)).ok_or_else(super::size_error);
        Ok(Some(match expr {
            Expr::Column { id, .. } => match schema.position(*id) { Some(position) => batch.columns[position].retained_bytes()?, None => return Ok(None) },
            Expr::Literal(value) if literal(value) => super::repeated_cell_bound(value, rows)?,
            Expr::Parameter { .. } => 0,
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
                let Some((_, _, array, field, logical)) = struct_field(expr, name, schema, batch) else { return Ok(None); };
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
                let Some((_, parent, child, field, logical)) = struct_field(expr, name, schema, batch) else { return Ok(None); };
                Ok(project_struct_child(parent, child, field, logical))
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
                Ok(identity_cast(&column, target).then_some(column))
            }
            Expr::RowCopy(fields) => row_copy(fields, schema, batch),
            _ => Ok(None),
        }
    }

    fn identity_cast(column: &Column, target: &LogicalType) -> bool {
        let Column::Arrow { logical: Some(logical), .. } = column else { return false; };
        let target = target.unencoded();
        let target = match &target { LogicalType::Maybe(inner) => inner.as_ref(), target => target };
        super::plain_scalar(logical).is_some_and(|scalar| super::copyable(scalar) && *target == LogicalType::Scalar(scalar.clone()))
    }

    fn struct_field<'a>(expr: &Expr, name: &str, schema: &Schema, batch: &'a ColumnBatch)
        -> Option<(usize, &'a StructArray, &'a ArrayRef, &'a Arc<ArrowField>, &'a LogicalType)> {
        let Expr::Column { id, .. } = expr else { return None; };
        let input = schema.position(*id)?;
        let Column::Arrow { array, field, logical: Some(logical) } = &batch.columns[input] else { return None; };
        let (parent, child, field, logical) = struct_child(array, field, logical, name)?;
        Some((input, parent, child, field, logical))
    }

    fn struct_child<'a>(array: &'a ArrayRef, field: &ArrowField, logical: &'a LogicalType, name: &str)
        -> Option<(&'a StructArray, &'a ArrayRef, &'a Arc<ArrowField>, &'a LogicalType)> {
        if !super::stable_read_layout(logical, field) { return None; }
        let logical = match logical { LogicalType::Maybe(inner) => inner.as_ref(), logical => logical };
        let LogicalType::Struct(types) = logical else { return None; };
        let logical = types.get(name)?;
        if *logical == LogicalType::Nothing { return None; }
        let parent = array.as_any().downcast_ref::<StructArray>()?;
        let position = parent.fields().iter().position(|field| field.name() == name)?;
        Some((parent, parent.column(position), &parent.fields()[position], logical))
    }

    fn project_struct_child(parent: &StructArray, child: &ArrayRef, field: &Arc<ArrowField>, logical: &LogicalType) -> Option<Column> {
        if parent.null_count() == 0 || child.data_type() == &DataType::Null {
            return Some(Column::Arrow { array: Arc::clone(child), field: Arc::clone(field), logical: Some(logical.clone()) });
        }
        if matches!(child.data_type(), DataType::Union(..) | DataType::RunEndEncoded(..)) { return None; }
        let nulls = NullBuffer::union(parent.nulls(), child.nulls());
        let data = child.to_data().into_builder().nulls(nulls).build().ok()?;
        let array = make_array(data);
        let field = Arc::new(field.as_ref().clone().with_nullable(true));
        Some(Column::Arrow { array, field, logical: Some(logical.clone().maybe()) })
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

    pub(super) fn coalesce_selection(
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
        fn layout(field: &ArrowField, logical: Option<&LogicalType>) -> bool {
            let Some(logical) = logical.filter(|logical| super::stable_read_layout(logical, field)) else { return false; };
            match field.data_type() {
                DataType::Null => field.is_nullable(),
                DataType::Union(fields, arrow_schema::UnionMode::Dense) if field.metadata().is_empty() => fields.iter().all(|(_, child)| {
                    super::scalar_layout(child.data_type()) && rtdb_columnar::from_field(child).is_ok_and(|ty| {
                        rtdb_columnar::leg_name(&ty).is_ok_and(|name| name == *child.name()) && layout(child, Some(&ty))
                    })
                }),
                DataType::List(child) | DataType::FixedSizeList(child, _) if field.metadata().is_empty() && child.metadata().is_empty() => {
                    matches!(child.data_type(), DataType::Float32 | DataType::Float64)
                        || rtdb_types::ListValue::supports_float_union(child.data_type())
                }
                data_type => super::scalar_layout(data_type) && super::plain_scalar(logical).is_some_and(super::copyable),
            }
        }
        let mut direct_layout = None;
        let mut needs_selection = false;
        let mut literal_tail = None;
        for arg in args {
            let position = match arg {
                Expr::Literal(Value::Null | Value::Undefined) => continue,
                Expr::Literal(value) if literal(value) => { literal_tail = Some(value); break; }
                Expr::Column { id, .. } => match schema.position(*id) { Some(position) => position, None => return Ok(None) },
                _ => return Ok(None),
            };
            let column = &batch.columns[position];
            let compatible = match column {
                Column::Arrow { field, logical, .. } => {
                    let data_type = field.data_type();
                    let physical_and_scalar = (data_type, logical.as_ref().and_then(super::plain_scalar));
                    needs_selection |= !super::scalar_layout(data_type) || *direct_layout.get_or_insert(physical_and_scalar) != physical_and_scalar;
                    layout(field, logical.as_ref())
                }
                Column::Selected(column) => {
                    needs_selection = true;
                    column.source_layouts().all(|(_, field, logical)| layout(field, logical))
                }
                Column::Values(_) => false,
            };
            if !compatible { return Ok(None); }
            inputs.push((column, batch.column_reservation(position)));
        }
        if !needs_selection { return Ok(None); }
        let tail = if let Some(value) = literal_tail {
            let Ok(bound) = super::repeated_cell_bound(value, 1) else { return Ok(None); };
            let Ok(mut owner) = memory.reserve(bound) else { return Ok(None); };
            let Ok(column) = super::repeat_cell(value, 1) else { return Ok(None); };
            Arc::get_mut(&mut owner).expect("The COALESCE literal owns its reservation")
                .shrink_to(super::owned_projection_bytes(&column)?)?;
            Some((column, owner))
        } else { None };
        let picks = (0..batch.len).map(|row| inputs.iter().find_map(|&(column, charge)| {
            let (array, field, _, offset) = column.arrow_cell(row)??;
            (super::arrow_cell_is_null(field, array, offset) == Some(false)).then_some((column, charge, row))
        }).or_else(|| tail.as_ref().map(|(column, charge)| (column, charge, 0))));
        let selection = super::take_arrow_rows(picks, batch.len, true, memory, None).and_then(|(column, mut charge)| {
            let bytes = column.retained_bytes()?;
            let lease = Arc::get_mut(&mut charge).expect("The COALESCE selection owns its reservation");
            if bytes <= lease.bytes() { lease.shrink_to(bytes)?; } else { lease.grow_to(bytes)?; }
            Ok((column, charge))
        });
        match selection {
            Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => Ok(None),
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
fn arrow_coalesce_text_lane_retains_both_vector_layouts() {
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
    let (column, _charge) = projection::coalesce_selection(&expr, &schema, &batch, &memory).unwrap().unwrap();
    for (row, expected) in [left, right].into_iter().enumerate() {
        let (input, ..) = batch.columns[row].arrow_cell(row).unwrap().unwrap();
        let (retained, ..) = column.arrow_cell(row).unwrap().unwrap();
        assert!(std::ptr::eq(retained, input));
        assert_eq!(column.value(row).unwrap(), expected);
    }
    assert_eq!(column.value(2).unwrap(), Value::Null);
}

#[test]
fn arrow_coalesce_text_lane_retains_selected_scalar_winners_and_custody() {
    use rtdb_plan::{ColumnId, Expr, Schema};

    let memory = MemoryBudget::new(1 << 20);
    let text = Value::Utf8("e\u{301}\0😀".into());
    let number = Value::I32(7);
    let types = [text.logical_type().join(&LogicalType::Scalar(ScalarType::Bool)).maybe(), number.logical_type().maybe()];
    let schema = Schema::new(types.iter().enumerate().map(|(index, ty)| Field {
        id: ColumnId(index as u32), name: format!("input{index}"), qualifier: None,
        qualified_only: false, unnamed: false, data_type: ty.clone(),
    }).collect()).unwrap();
    let text_array = rtdb_columnar::copy_array(&types[0], &[Value::Null, text.clone(), Value::Bool(true)]).unwrap();
    let number_array = rtdb_columnar::copy_array(&types[1], &[Value::I32(1), number.clone(), Value::Null]).unwrap();
    let selected = Column::Selected(SelectedColumn::new(vec![Some((
        Arc::clone(&text_array), Arc::new(rtdb_columnar::to_field("input0", &types[0]).unwrap()), Some(types[0].clone()), None,
    )), None], Arc::new(vec![(0, 1), (1, 0), (0, 0)])));
    let direct = Column::Arrow {
        array: Arc::clone(&number_array), field: Arc::new(rtdb_columnar::to_field("input1", &types[1]).unwrap()), logical: Some(types[1].clone()),
    };
    let batch = ColumnBatch::settled(vec![selected, direct], 3, memory.reserve(4096).unwrap()).unwrap();
    let backing = Arc::downgrade(batch.column_reservation(0));
    let args = schema.fields().iter().map(Field::expression).collect::<Vec<_>>();
    let expression = Expr::Coalesce(args.clone());
    let (column, charge) = projection::coalesce_selection(&expression, &schema, &batch, &memory).unwrap().unwrap();
    for (row, source) in [text_array, number_array].iter().enumerate() {
        let (array, _, _, offset) = column.arrow_cell(row).unwrap().unwrap();
        assert!(std::ptr::eq(array, source.as_ref()));
        assert_eq!(offset, 1);
    }
    let ty = expression.data_type();
    let field = rtdb_columnar::to_field("output", &ty).unwrap();
    let (array, output_charge) = relayout_output(&column, &ty, &field, &memory).unwrap().unwrap();
    for (row, value) in [text.clone(), number.clone(), Value::Null].into_iter().enumerate() {
        assert_eq!(rtdb_columnar::read_value(&field, array.as_ref(), row).unwrap(), value);
    }

    let tail = Value::Utf8("fallback".into());
    let expression = Expr::Coalesce([args, vec![Expr::Literal(tail.clone())]].concat());
    let (with_tail, tail_charge) = projection::coalesce_selection(&expression, &schema, &batch, &memory).unwrap().unwrap();
    drop(batch);
    assert!(backing.upgrade().is_some());
    assert!(matches!(with_tail, Column::Selected(_)));
    assert_eq!(with_tail.value(0).unwrap(), text);
    assert_eq!(with_tail.value(1).unwrap(), number);
    assert_eq!(with_tail.value(2).unwrap(), tail);
    drop((with_tail, tail_charge, column, charge, array, output_charge));
    assert!(backing.upgrade().is_none());
    assert_eq!(memory.used_bytes(), 0);
}

#[cfg(test)]
mod integer_comparison_tests {
    use super::*;
    use rtdb_plan::{BinaryOp, ColumnId, Expr, Schema};
    use rtdb_types::{LogicalType, ScalarType, Timestamp, Value};

    #[tokio::test]
    async fn an_integer_column_compared_with_a_constant_keeps_nulls_unknown_either_way_round() {
        let memory = MemoryBudget::new(1 << 18);
        let ty = LogicalType::Scalar(ScalarType::I64).maybe();
        let field = Arc::new(rtdb_columnar::to_field("n", &ty).unwrap());
        let array: arrow_array::ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![Some(1), None, Some(5), Some(9)]));
        let batch = ColumnBatch::settled(vec![Column::Arrow { field, array, logical: Some(ty.clone()) }], 4, memory.reserve(0).unwrap()).unwrap();
        let schema = Schema::new(vec![rtdb_plan::Field { id: ColumnId(0), name: "n".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: ty }]).unwrap();
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
        let column = schema.fields()[0].expression();
        let five = Expr::Literal(Value::I64(5));
        for (predicate, expected) in [
            (Expr::binary(BinaryOp::GtEq, column.clone(), five.clone()), [Some(false), None, Some(true), Some(true)]),
            (Expr::binary(BinaryOp::Lt, five.clone(), column.clone()), [Some(false), None, Some(false), Some(true)]),
            (Expr::binary(BinaryOp::Eq, column.clone(), five.clone()), [Some(false), None, Some(true), Some(false)]),
            (Expr::binary(BinaryOp::NotEq, five, column), [Some(true), None, Some(false), Some(true)]),
        ] {
            let (truths, _charge) = predicate_truth(&batch, &predicate, &schema, &execution).await.unwrap();
            assert_eq!(truths, expected, "{predicate:?}");
        }
    }
}
