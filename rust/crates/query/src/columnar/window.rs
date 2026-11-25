use std::{mem::size_of, ops::Range, sync::Arc};

use arrow_array::ArrayRef;
use arrow_schema::{DataType, Field as ArrowField};
use rtdb_columnar::MemoryReservation;
use rtdb_types::{LogicalType, Result};

use super::{Column, ColumnBatch, RowExpansion, SelectedColumn, SelectedRows, StoredVector, size_error};
use crate::QueryContext;

#[derive(Clone)]
pub(crate) enum WindowSpan<'a> {
    Null(usize),
    Source(&'a ColumnBatch, Range<usize>),
}

struct Run {
    end: usize,
    source_start: Option<usize>,
}

pub(super) struct WindowSpans {
    runs: Vec<Run>,
    reservation: Arc<MemoryReservation>,
}

impl WindowSpans {
    pub(super) fn reservation(&self) -> &Arc<MemoryReservation> {
        &self.reservation
    }

    fn run_at(&self, row: usize) -> usize {
        self.runs.partition_point(|run| run.end <= row)
    }

    fn start(&self, run: usize) -> usize {
        run.checked_sub(1).map_or(0, |previous| self.runs[previous].end)
    }

    pub(super) fn position(&self, row: usize) -> (usize, usize) {
        let run = self.run_at(row);
        (run, self.runs[run].source_start.map_or(0, |start| start + (row - self.start(run))))
    }

    pub(super) fn contains(&self, run: usize, end: usize) -> bool {
        let run = &self.runs[run];
        run.source_start.is_some() && end <= run.end
    }

    pub(super) fn iter(&self, rows: Range<usize>) -> impl Iterator<Item = (usize, usize)> + '_ {
        let first = self.run_at(rows.start);
        self.runs[first..].iter().enumerate()
            .map(move |(index, run)| (first + index, run, self.start(first + index)))
            .take_while(move |&(_, _, start)| start < rows.end)
            .flat_map(move |(index, run, start)| {
                (rows.start.max(start)..rows.end.min(run.end))
                    .map(move |row| (index, run.source_start.map_or(0, |first| first + (row - start))))
            })
    }
}

pub(super) type WindowVector<'a> = (
    &'a ArrayRef, &'a Arc<ArrowField>, &'a Option<LogicalType>, &'a Arc<MemoryReservation>,
);

impl ColumnBatch {
    pub(super) fn window_span_source(
        &self, position: usize, rows: Range<usize>,
    ) -> Option<(WindowVector<'_>, Range<usize>)> {
        let owner = self.column_reservation(position);
        let (array, field, logical, owner, rows) = match &self.columns[position] {
            Column::Arrow { array, field, logical } if rows.end <= array.len() => (array, field, logical, owner, rows),
            Column::Selected(selection) => {
                let ((array, field, logical, custody), rows) = selection.contiguous_source(rows)?;
                (array, field, logical, custody.as_ref().unwrap_or(owner), rows)
            }
            _ => return None,
        };
        (array.data_type() != &DataType::Null).then_some(((array, field, logical, owner), rows))
    }

    pub(crate) fn remove_window_partition(&mut self, position: usize) -> (Column, Option<Arc<MemoryReservation>>) {
        let appended = position.checked_sub(self.columns.len() - self.appended.len());
        (self.columns.remove(position), appended.map(|index| self.appended.remove(index)))
    }
}

pub(crate) fn lend_window_spans<'a>(
    spans: impl Iterator<Item = WindowSpan<'a>> + Clone, position: usize, rows: usize, context: &QueryContext,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let mut vectors = 0usize;
    let mut null_runs = 0usize;
    let mut in_null_run = false;
    let mut layout = None;
    for span in spans.clone() {
        let WindowSpan::Source(batch, rows) = span else {
            null_runs += usize::from(!in_null_run);
            in_null_run = true;
            continue;
        };
        let Some(((array, field, logical, _), _)) = batch.window_span_source(position, rows) else {
            return Ok(None);
        };
        if array.data_type() != field.data_type() || *layout.get_or_insert((field, logical)) != (field, logical) {
            return Ok(None);
        }
        vectors += 1;
        in_null_run = false;
    }
    for row in 0..rows {
        context.check_cancelled_at(row)?;
    }

    let count = vectors + null_runs;
    let logical_bytes = layout.and_then(|(_, logical)| logical.as_ref()).map_or(Ok(0), LogicalType::retained_bytes)?;
    let run_bytes = count.checked_mul(size_of::<Run>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
    let vector_bytes = count.checked_mul(size_of::<Option<StoredVector>>())
        .and_then(|bytes| bytes.checked_add(vectors.checked_mul(logical_bytes)?))
        .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
    let mut charge = context.memory.reserve(run_bytes.checked_add(vector_bytes).ok_or_else(size_error)?)?;
    let reservation = Arc::get_mut(&mut charge).expect("Window owns the span reservation").split_off(run_bytes)?;
    let mut runs: Vec<Run> = Vec::with_capacity(count);
    let mut sources = Vec::with_capacity(count);
    let mut end = 0;
    for span in spans {
        let (len, start, source) = match span {
            WindowSpan::Null(len) => (len, None, None),
            WindowSpan::Source(batch, rows) => {
                let ((array, field, logical, owner), rows) = batch.window_span_source(position, rows)
                    .expect("The window admitted this source span");
                let source = (Arc::clone(array), Arc::clone(field), logical.clone(), Some(Arc::clone(owner)));
                (rows.len(), Some(rows.start), Some(source))
            }
        };
        end += len;
        if start.is_none() && let Some(run) = runs.last_mut() && run.source_start.is_none() {
            run.end = end;
            continue;
        }
        runs.push(Run { end, source_start: start });
        sources.push(source);
    }
    let expansion = RowExpansion::WindowSpans(Arc::new(WindowSpans { runs, reservation }));
    let cells = SelectedRows::Expanded { expansion, rows: 0..end, input: None };
    Ok(Some((Column::Selected(SelectedColumn { sources: Arc::new(sources), cells }), charge)))
}
