use std::{mem::size_of, ops::Range, sync::Arc};

use arrow_array::{Array, ArrayRef, ListArray, StructArray};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field as ArrowField};
use rtdb_columnar::{MemoryBudget, MemoryReservation, retain_array, retain_buffer};
use rtdb_plan::{Field, Schema};
use rtdb_types::{LogicalType, Result};

use super::{
    Column, ColumnBatch, RowExpansion, SelectedColumn, SelectedRows, StoredVector, expanded_column, expansion_source,
    copyable, gather_column_cells, pack_evaluated_column, plain_scalar, size_error, stable_read_layout,
};
use crate::invalid;

struct ApplySpan {
    end: usize,
    outer: usize,
    page: Option<usize>,
}

pub(super) struct ApplySpans {
    spans: Vec<ApplySpan>,
    reservation: Arc<MemoryReservation>,
}

impl ApplySpans {
    fn len(&self) -> usize {
        self.spans.last().map_or(0, |span| span.end)
    }

    fn start(&self, index: usize) -> usize {
        index.checked_sub(1).map_or(0, |previous| self.spans[previous].end)
    }

    fn index(&self, row: usize) -> usize {
        self.spans.partition_point(|span| span.end <= row)
    }

    pub(super) fn reservation(&self) -> &Arc<MemoryReservation> {
        &self.reservation
    }

    pub(super) fn outer(&self, row: usize) -> usize {
        self.spans[self.index(row)].outer
    }

    pub(super) fn outer_range(&self, rows: Range<usize>) -> Option<Range<usize>> {
        if rows.is_empty() {
            return None;
        }
        let first = self.index(rows.start);
        let last = self.index(rows.end - 1);
        let outer = self.spans[first].outer;
        (last - first + 1 == rows.len()
            && self.spans[first..=last].iter().enumerate().all(|(index, span)| span.outer == outer + index))
        .then(|| outer..outer + rows.len())
    }
}

enum ApplyVector {
    Arrow(usize),
    Selected { first_source: usize, rows: SelectedRows, owners: Vec<Arc<MemoryReservation>> },
}

#[derive(Clone)]
pub(super) struct ApplySelection {
    spans: Arc<ApplySpans>,
    vectors: Arc<Vec<ApplyVector>>,
    rows: Range<usize>,
}

impl ApplySelection {
    pub(super) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(super) fn retained_bytes(&self) -> Result<usize> {
        let bytes = self.vectors.capacity().checked_mul(size_of::<ApplyVector>()).ok_or_else(size_error)?;
        self.vectors.iter().try_fold(bytes, |bytes, vector| match vector {
            ApplyVector::Arrow(_) => Ok(bytes),
            ApplyVector::Selected { owners, .. } => bytes
                .checked_add(owners.capacity().checked_mul(size_of::<Arc<MemoryReservation>>()).ok_or_else(size_error)?)
                .ok_or_else(size_error),
        })
    }

    pub(super) fn owners(&self) -> impl Iterator<Item = &Arc<MemoryReservation>> {
        std::iter::once(&self.spans.reservation).chain(self.vectors.iter().flat_map(|vector| match vector {
            ApplyVector::Arrow(_) => &[],
            ApplyVector::Selected { owners, .. } => owners.as_slice(),
        }))
    }

    pub(super) fn slice(&self, rows: Range<usize>) -> Self {
        Self { rows: self.rows.start + rows.start..self.rows.start + rows.end, ..self.clone() }
    }

    fn position(&self, span: &ApplySpan, offset: usize) -> (usize, usize) {
        let Some(vector) = span.page else {
            return (0, 0);
        };
        match &self.vectors[vector] {
            ApplyVector::Arrow(source) => (*source, offset),
            ApplyVector::Selected { first_source, rows, .. } => {
                let (source, row) = rows.get(offset).expect("The dependent selection covers its page");
                (first_source + source, row)
            }
        }
    }

    pub(super) fn get(&self, row: usize) -> Option<(usize, usize)> {
        if row >= self.len() {
            return None;
        }
        let row = self.rows.start + row;
        let index = self.spans.index(row);
        Some(self.position(&self.spans.spans[index], row - self.spans.start(index)))
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        let first = self.spans.index(self.rows.start);
        self.spans
            .spans
            .iter()
            .enumerate()
            .skip(first)
            .take_while(|&(index, _)| self.spans.start(index) < self.rows.end)
            .flat_map(move |(index, span)| {
                let start = self.spans.start(index);
                let rows = self.rows.start.saturating_sub(start)..span.end.min(self.rows.end) - start;
                rows.map(move |offset| self.position(span, offset))
            })
    }

    pub(super) fn contiguous(&self, rows: Range<usize>) -> Option<(usize, Range<usize>)> {
        let start = self.rows.start + rows.start;
        let end = self.rows.start + rows.end;
        let index = self.spans.index(start);
        let span = &self.spans.spans[index];
        if end > span.end {
            return None;
        }
        let start_of_page = self.spans.start(index);
        let rows = start - start_of_page..end - start_of_page;
        match &self.vectors[span.page?] {
            ApplyVector::Arrow(source) => Some((*source, rows)),
            ApplyVector::Selected { first_source, rows: input, .. } => {
                let (source, rows) = input.contiguous(rows)?;
                Some((first_source + source, rows))
            }
        }
    }
}

pub(crate) struct ApplyPages {
    spans: Arc<ApplySpans>,
    pages: Vec<ColumnBatch>,
    pages_reservation: Arc<MemoryReservation>,
}

fn admit(reservation: &mut Arc<MemoryReservation>, bytes: usize) -> Result<()> {
    let lease = Arc::get_mut(reservation).expect("Apply owns its construction reservation");
    lease.grow_to(lease.bytes().checked_add(bytes).ok_or_else(size_error)?)
}

impl ApplyPages {
    pub(crate) fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        let spans = Arc::new(ApplySpans { spans: Vec::new(), reservation: memory.reserve(256)? });
        Ok(Self { spans, pages: Vec::new(), pages_reservation: memory.reserve(256)? })
    }

    pub(crate) fn len(&self) -> usize {
        self.spans.len()
    }

    pub(crate) fn end_after(&self, rows: usize) -> Result<usize> {
        self.len()
            .checked_add(rows)
            .filter(|rows| *rows <= i32::MAX as usize)
            .ok_or_else(|| invalid("row-size", "Apply output has too many rows"))
    }

    pub(crate) fn push(&mut self, outer: usize, page: Option<ColumnBatch>) -> Result<()> {
        let end = self.end_after(page.as_ref().map_or(1, |page| page.len))?;
        let spans = Arc::get_mut(&mut self.spans).expect("Apply owns its spans until projection");
        admit(&mut spans.reservation, 4 * size_of::<ApplySpan>())?;
        let page = if let Some(page) = page {
            admit(&mut self.pages_reservation, 4 * size_of::<ColumnBatch>())?;
            let index = self.pages.len();
            self.pages.push(page);
            Some(index)
        } else {
            None
        };
        spans.spans.push(ApplySpan { end, outer, page });
        Ok(())
    }

    pub(crate) fn span_count(&self) -> usize {
        self.spans.spans.len()
    }

    pub(crate) fn repeat(&mut self, outer: usize, spans: Range<usize>, context: &crate::QueryContext) -> Result<()> {
        for index in spans {
            context.check_cancelled_at(index)?;
            let span = &self.spans.spans[index];
            let end = self.end_after(span.end - self.spans.start(index))?;
            let page = span.page;
            let spans = Arc::get_mut(&mut self.spans).expect("Apply owns its spans until projection");
            admit(&mut spans.reservation, 4 * size_of::<ApplySpan>())?;
            spans.spans.push(ApplySpan { end, outer, page });
        }
        Ok(())
    }

    pub(crate) fn joined(
        &self,
        outer: &ColumnBatch,
        width: usize,
        context: &crate::QueryContext,
    ) -> Result<ColumnBatch> {
        let memory = &context.memory;
        let outer_span = self.spans.outer_range(0..self.len());
        let expansion = RowExpansion::ApplyOuter(Arc::clone(&self.spans));
        let mut output = ColumnBatch::settled(Vec::new(), self.len(), memory.reserve(256)?)?;
        for position in 0..outer.columns.len() {
            context.check_cancelled()?;
            let view = match &outer_span {
                Some(rows) => outer.lend_window_span(position, rows.clone(), memory)?,
                None => None,
            };
            let (column, charge) = match view {
                Some(view) => view,
                None if matches!(outer.columns[position], Column::Values(_)) => {
                    let rows = self.spans.spans.iter().enumerate().flat_map(|(index, span)| {
                        std::iter::repeat_n(Some((0, span.outer)), span.end - self.spans.start(index))
                    });
                    gather_column_cells(|_| outer, rows, self.len(), position, false, memory)?
                }
                None => expanded_column(
                    &outer.columns[position],
                    outer.column_reservation(position),
                    &expansion,
                    self.len(),
                    memory,
                )?,
            };
            output.append_column(column, charge, memory)?;
        }
        for position in 0..width {
            context.check_cancelled()?;
            let (column, charge) = self.dependent_column(position, memory)?;
            output.append_column(column, charge, memory)?;
        }
        Ok(output)
    }

    pub(crate) fn dependent_column(
        &self,
        position: usize,
        memory: &Arc<MemoryBudget>,
    ) -> Result<(Column, Arc<MemoryReservation>)> {
        if let [span] = self.spans.spans.as_slice()
            && let Some(page) = span.page
            && let Some(view) = self.pages[page].lend_window_span(position, 0..self.len(), memory)?
        {
            return Ok(view);
        }
        self.column(position, memory)
    }

    fn column(&self, position: usize, memory: &Arc<MemoryBudget>) -> Result<(Column, Arc<MemoryReservation>)> {
        let bytes = self
            .pages
            .len()
            .checked_mul(size_of::<ApplyVector>())
            .and_then(|bytes| bytes.checked_add(512))
            .ok_or_else(size_error)?;
        let mut reservation = memory.reserve(bytes)?;
        let mut vectors = Vec::with_capacity(self.pages.len());
        let mut sources = vec![None];
        for page in &self.pages {
            let packed;
            let (column, owner) = match &page.columns[position] {
                Column::Values(values) => {
                    packed = pack_evaluated_column(values, memory)?;
                    (&packed.0, &packed.1)
                }
                column => (column, page.column_reservation(position)),
            };
            let vector = match column {
                Column::Arrow { array, field, logical } => {
                    admit_vector(&mut reservation, logical.as_ref())?;
                    let index = sources.len();
                    sources.push(expansion_source(array, field, logical, owner));
                    ApplyVector::Arrow(index)
                }
                Column::Selected(selected) => {
                    let first_source = sources.len();
                    for source in selected.sources.iter() {
                        admit_vector(
                            &mut reservation,
                            source.as_ref().and_then(|(_, _, logical, _)| logical.as_ref()),
                        )?;
                        sources.push(source.as_ref().map(|(array, field, logical, custody)| {
                            (
                                Arc::clone(array),
                                Arc::clone(field),
                                logical.clone(),
                                Some(Arc::clone(custody.as_ref().unwrap_or(owner))),
                            )
                        }));
                    }
                    let count = selected.cells.expansion_owners().count().checked_add(1).ok_or_else(size_error)?;
                    admit(
                        &mut reservation,
                        count.checked_mul(size_of::<Arc<MemoryReservation>>()).ok_or_else(size_error)?,
                    )?;
                    let mut owners = Vec::with_capacity(count);
                    owners.push(Arc::clone(owner));
                    owners.extend(selected.cells.expansion_owners().cloned());
                    ApplyVector::Selected { first_source, rows: selected.cells.clone(), owners }
                }
                Column::Values(_) => unreachable!("Evaluated Apply columns were packed"),
            };
            vectors.push(vector);
        }
        let cells = SelectedRows::Apply(ApplySelection {
            spans: Arc::clone(&self.spans),
            vectors: Arc::new(vectors),
            rows: 0..self.len(),
        });
        Ok((Column::Selected(SelectedColumn { sources: Arc::new(sources), cells }), reservation))
    }
}

fn admit_vector(reservation: &mut Arc<MemoryReservation>, logical: Option<&LogicalType>) -> Result<()> {
    let bytes = logical
        .map_or(Ok(0), LogicalType::retained_bytes)?
        .checked_add(4 * size_of::<Option<StoredVector>>())
        .ok_or_else(size_error)?;
    admit(reservation, bytes)
}

impl ColumnBatch {
    pub(crate) fn apply_document(
        &self,
        positions: &[usize],
        schema: &Schema,
        memory: &Arc<MemoryBudget>,
    ) -> Result<Self> {
        let base = crate::runtime_schema::schema_bound(schema)?;
        let mut reservation = memory.reserve(base)?;
        let (fields, arrays, fallback) = match self.apply_document_fields(positions, schema, &mut reservation, memory)? {
            Some((fields, arrays)) => (fields.into(), arrays, None),
            None => {
                Arc::get_mut(&mut reservation).expect("Apply owns its document reservation").shrink_to(base)?;
                let projection = self.key_projection(positions, memory)?;
                let output = crate::cursor::column_batch(schema, memory, projection)?;
                (output.batch().schema_ref().fields().clone(), output.batch().columns().to_vec(), Some(Arc::clone(output.reservation())))
            }
        };
        let record = StructArray::try_new_with_length(fields, arrays, None, self.len)
            .map_err(|error| invalid("apply-document", error))?;
        let field = Arc::new(ArrowField::new("doc", record.data_type().clone(), false));
        let logical = rtdb_columnar::from_field(&field)?;
        admit(&mut reservation, field.size().checked_add(logical.retained_bytes()?).ok_or_else(size_error)?)?;
        if let Some(fallback) = fallback {
            reservation = memory.reserve_with_backing(0, [reservation, fallback].into_iter())?;
        }
        Ok(Self::sharing(
            vec![Column::Arrow { array: Arc::new(record), field, logical: Some(logical) }],
            self.len,
            reservation,
        ))
    }

    fn apply_document_fields(
        &self,
        positions: &[usize],
        schema: &Schema,
        reservation: &mut Arc<MemoryReservation>,
        memory: &Arc<MemoryBudget>,
    ) -> Result<Option<(Vec<ArrowField>, Vec<ArrayRef>)>> {
        let mut fields = Vec::with_capacity(positions.len());
        let mut arrays = Vec::with_capacity(positions.len());
        let mut all_declared_layouts = None;
        for (&position, output) in positions.iter().zip(schema.fields()) {
            let owner = self.column_reservation(position);
            let (array, field, logical, custody) = match &self.columns[position] {
                Column::Arrow { array, field, logical } => {
                    (Arc::clone(array), Arc::clone(field), logical.clone(), Arc::clone(owner))
                }
                Column::Selected(selected) => {
                    if let Some(((array, field, logical, custody), rows)) = selected.contiguous_source(0..self.len) {
                        let array = if rows.len() == array.len() {
                            Arc::clone(array)
                        } else {
                            array.slice(rows.start, rows.len())
                        };
                        (array, Arc::clone(field), logical.clone(), Arc::clone(custody.as_ref().unwrap_or(owner)))
                    } else {
                        match selected.uniform_fragment_vector(0..self.len, memory)? {
                            Some(((array, field, logical), charge)) => (array, field, logical, charge),
                            None => return Ok(None),
                        }
                    }
                }
                Column::Values(values) => {
                    let (column, charge) = pack_evaluated_column(values, memory)?;
                    let Column::Arrow { array, field, logical } = column else {
                        return Ok(None);
                    };
                    (array, field, logical, charge)
                }
            };
            let compatible = match logical.as_ref() {
                Some(ty) => *ty != LogicalType::Nothing && stable_read_layout(ty, &field),
                None => *all_declared_layouts.get_or_insert_with(|| positions.iter().zip(schema.fields()).all(|(&position, output)| {
                    let (array, field, logical, mut rows) = match &self.columns[position] {
                        Column::Arrow { array, field, logical } if array.len() == self.len => (array, field, logical, 0..self.len),
                        Column::Selected(selected) => {
                            let Some(((array, field, logical, _), rows)) = selected.contiguous_source(0..self.len)
                                else { return false; };
                            (array, field, logical, rows)
                        }
                        _ => return false,
                    };
                    field.metadata().is_empty() && array.data_type() == field.data_type()
                        && (field.is_nullable() || array.null_count() == 0 || rows.all(|row| array.is_valid(row)))
                        && logical.as_ref().is_none_or(|ty| *ty == output.data_type)
                        && (plain_scalar(&output.data_type).is_some_and(copyable)
                            || matches!(field.data_type(), DataType::List(child) | DataType::FixedSizeList(child, _)
                                if super::lists::captured_list_child(child)))
                        && stable_read_layout(&output.data_type, field)
                        && rtdb_columnar::to_field(field.name(), &output.data_type).is_ok_and(|declared| declared == **field)
                })),
            };
            if !compatible {
                return Ok(None);
            }
            let descriptors = array.get_array_memory_size().saturating_sub(array.get_buffer_memory_size());
            let bytes =
                descriptors.checked_add(field.size()).and_then(|bytes| bytes.checked_mul(4)).ok_or_else(size_error)?;
            admit(reservation, bytes)?;
            arrays.push(retain_array(array, &custody)?);
            fields.push(field.as_ref().clone().with_name(output.name.clone()));
        }
        Ok(Some((fields, arrays)))
    }

    pub(crate) fn append_apply_lists(
        &mut self,
        pages: &ApplyPages,
        offsets: Vec<i32>,
        valid: Vec<bool>,
        output: &Field,
        mut reservation: Arc<MemoryReservation>,
        memory: &Arc<MemoryBudget>,
    ) -> Result<()> {
        let type_bytes = output.data_type.retained_bytes()?.checked_mul(4).ok_or_else(size_error)?;
        admit(&mut reservation, type_bytes)?;
        let physical = rtdb_columnar::to_field(&output.name, &output.data_type)?;
        let DataType::List(item) = physical.data_type() else {
            return Err(invalid("column-list", "Expected a list result field"));
        };
        let (item, child) = pages.list_child(item, &mut reservation, memory)?;
        admit(&mut reservation, item.size().checked_mul(4).ok_or_else(size_error)?)?;
        let offsets = ScalarBuffer::from(offsets);
        let offsets =
            OffsetBuffer::new(ScalarBuffer::new(retain_buffer(offsets.inner(), &reservation), 0, offsets.len()));
        let nulls = NullBuffer::from(valid);
        let nulls = NullBuffer::new(BooleanBuffer::new(
            retain_buffer(nulls.buffer(), &reservation),
            nulls.offset(),
            nulls.len(),
        ));
        let array =
            ListArray::try_new(item, offsets, child, Some(nulls)).map_err(|error| invalid("column-list", error))?;
        let field = Arc::new(ArrowField::new(
            physical.name(),
            array.data_type().clone(),
            physical.is_nullable() || array.null_count() != 0,
        ));
        let logical = rtdb_columnar::from_field(&field)?;
        self.append_funded_column(
            Column::Arrow { array: Arc::new(array), field, logical: Some(logical) },
            reservation,
            memory,
        )
    }
}

impl ApplyPages {
    fn list_child(&self, item: &Arc<ArrowField>, reservation: &mut Arc<MemoryReservation>, memory: &Arc<MemoryBudget>) -> Result<(Arc<ArrowField>, ArrayRef)> {
        if self.pages.is_empty() {
            return Ok((Arc::clone(item), arrow_array::new_empty_array(item.data_type())));
        }
        let stable = |column: &Column| {
            matches!(column,
            Column::Arrow { field, logical: Some(ty), .. } if stable_read_layout(ty, field))
        };
        if self.span_count() == 1
            && let [page] = self.pages.as_slice()
            && let Column::Arrow { array, field, .. } = &page.columns[0]
            && stable(&page.columns[0])
        {
            let descriptors = array.get_array_memory_size().saturating_sub(array.get_buffer_memory_size());
            admit(reservation, descriptors.checked_mul(4).ok_or_else(size_error)?)?;
            let array = retain_array(Arc::clone(array), page.column_reservation(0))?;
            return Ok((Arc::new(field.as_ref().clone().with_name(item.name())), array));
        }

        let (column, _inventory) = self.column(0, memory)?;
        let Column::Selected(selected) = column else {
            unreachable!("Apply documents are Arrow structs");
        };
        if self.pages.iter().all(|page| stable(&page.columns[0])) {
            match selected.uniform_fragment_vector(0..self.len(), memory) {
                Ok(Some(((array, field, _), charge))) => {
                    let field = Arc::new(field.as_ref().clone().with_name(item.name()));
                    return Ok((field, retain_array(array, &charge)?));
                }
                Ok(None) => {}
                Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => {}
                Err(error) => return Err(error),
            }
        }
        let (array, field, charge) = selected.list_values(item.name(), memory)?;
        Ok((field, retain_array(array, &charge)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, cast::AsArray};
    use rtdb_types::{ScalarType, Value};

    #[test]
    fn apply_document_shares_a_declared_list_without_cached_logical_metadata() {
        let memory = MemoryBudget::new(1 << 20);
        let source_memory = MemoryBudget::new(1 << 20);
        let logical = LogicalType::list(LogicalType::Scalar(ScalarType::Utf8).maybe()).maybe();
        let field = Arc::new(rtdb_columnar::to_field("values", &logical).unwrap());
        let values = [Value::Null, Value::list(vec![Value::Utf8("text".into()), Value::Null])];
        let array = rtdb_columnar::build_array(&logical, &values).unwrap();
        let address = array.as_list::<i32>().values().as_string::<i32>().value(0).as_ptr();
        let source = ColumnBatch::settled(vec![Column::Arrow { array, field, logical: None }],
            2, source_memory.reserve(4096).unwrap()).unwrap();
        let schema = Schema::new(vec![Field {
            id: rtdb_plan::ColumnId(0), name: "values".into(), data_type: logical,
            qualifier: None, qualified_only: false, unnamed: false,
        }]).unwrap();
        let document = source.apply_document(&[0], &schema, &memory).unwrap();
        drop(source);
        let [Column::Arrow { array, .. }] = document.columns.as_slice() else { panic!("Expected a document array"); };
        let lists = array.as_struct().column(0).as_list::<i32>();
        assert!(lists.is_null(0));
        let child = lists.value(1);
        assert_eq!(child.as_string::<i32>().iter().collect::<Vec<_>>(), [Some("text"), None]);
        assert_eq!(child.as_string::<i32>().value(0).as_ptr(), address);
        assert!(source_memory.used_bytes() > 0);
        drop((child, document));
        assert_eq!(source_memory.used_bytes(), 0);
    }

    #[test]
    fn apply_document_lends_a_contiguous_selected_list_without_cached_type() {
        let memory = MemoryBudget::new(1 << 20);
        let source_memory = MemoryBudget::new(1 << 20);
        let logical = LogicalType::list(LogicalType::Scalar(ScalarType::Utf8).maybe()).maybe();
        let field = Arc::new(rtdb_columnar::to_field("values", &logical).unwrap());
        let lists = rtdb_columnar::build_array(&logical, &[
            Value::list(vec![Value::Utf8("earlier".into())]),
            Value::Null,
            Value::list(vec![Value::Utf8("selected".into()), Value::Null]),
        ]).unwrap();
        let address = lists.as_list::<i32>().values().as_string::<i32>().value(1).as_ptr();
        let owner = source_memory.reserve(lists.get_array_memory_size()).unwrap();
        let selected = SelectedColumn::new(vec![Some((lists, field, None, Some(owner)))], Arc::new(vec![(0, 1), (0, 2)]));
        let int = LogicalType::Scalar(ScalarType::I64);
        let id_field = Arc::new(rtdb_columnar::to_field("id", &int).unwrap());
        let source = ColumnBatch::settled(vec![
            Column::Selected(selected),
            Column::Arrow { array: Arc::new(Int64Array::from(vec![11, 12])), field: id_field, logical: Some(int.clone()) },
        ], 2, memory.reserve(4096).unwrap()).unwrap();
        let schema = Schema::new([("id", int), ("values", logical)].into_iter().enumerate()
            .map(|(id, (name, data_type))| Field {
                id: rtdb_plan::ColumnId(id as u32), name: name.into(), data_type,
                qualifier: None, qualified_only: false, unnamed: false,
            }).collect()).unwrap();

        let document = source.apply_document(&[1, 0], &schema, &memory).unwrap();
        drop(source);
        let [Column::Arrow { array, .. }] = document.columns.as_slice() else { panic!("Expected a document array"); };
        let record = array.as_struct();
        assert_eq!(record.column_names(), ["id", "values"]);
        assert_eq!(record.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().as_ref(), &[11, 12]);
        let lists = record.column(1).as_list::<i32>();
        assert!(lists.is_null(0));
        let child = lists.value(1);
        let strings = child.as_string::<i32>();
        assert_eq!(strings.iter().collect::<Vec<_>>(), [Some("selected"), None]);
        assert_eq!(strings.value(0).as_ptr(), address);
        assert!(source_memory.used_bytes() > 0);
        drop((child, document));
        assert_eq!(source_memory.used_bytes(), 0);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn a_single_apply_document_span_funds_the_collected_list_child() {
        let memory = MemoryBudget::new(1 << 20);
        let source_memory = MemoryBudget::new(1 << 20);
        let field = ArrowField::new("n", DataType::Int64, false);
        let numbers = Int64Array::from(vec![17, 19]);
        let address = numbers.values().as_ptr();
        let array = Arc::new(StructArray::new(vec![field].into(), vec![Arc::new(numbers)], None));
        let field = Arc::new(ArrowField::new("doc", array.data_type().clone(), false));
        let logical = rtdb_columnar::from_field(&field).unwrap();
        let page = ColumnBatch::settled(vec![Column::Arrow { array, field, logical: Some(logical.clone()) }],
            2, source_memory.reserve(4096).unwrap()).unwrap();
        let mut pages = ApplyPages::new(&memory).unwrap();
        pages.push(0, Some(page)).unwrap();
        let output = Field {
            id: rtdb_plan::ColumnId(0), name: "docs".into(), data_type: LogicalType::list(logical).maybe(),
            qualifier: None, qualified_only: false, unnamed: false,
        };
        let mut result = ColumnBatch::settled(Vec::new(), 1, memory.reserve(256).unwrap()).unwrap();
        result.append_apply_lists(&pages, vec![0, 2], vec![true], &output, memory.reserve(512).unwrap(), &memory).unwrap();
        drop(pages);
        let [Column::Arrow { array, .. }] = result.columns.as_slice() else { panic!("Expected a document list"); };
        let list = array.as_list::<i32>();
        let child = list.values().as_struct().column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(child.values().as_ptr(), address);
        assert_eq!(child.values().as_ref(), [17, 19]);
        assert!(source_memory.used_bytes() > 0);
        drop(result);
        assert_eq!(source_memory.used_bytes(), 0);
    }

    #[test]
    fn apply_document_conforms_a_scalar_without_cached_logical_metadata_by_sharing() {
        let memory = MemoryBudget::new(1 << 20);
        let source_memory = MemoryBudget::new(1 << 20);
        let logical = LogicalType::Scalar(ScalarType::I64).maybe();
        let field = Arc::new(rtdb_columnar::to_field("value", &logical).unwrap());
        let array = Int64Array::from(vec![Some(17), None, Some(19)]);
        let values_ptr = array.values().as_ptr();
        let source = ColumnBatch::settled(vec![Column::Arrow { array: Arc::new(array), field, logical: None }],
            3, source_memory.reserve(4096).unwrap()).unwrap();
        let schema = Schema::new(vec![Field {
            id: rtdb_plan::ColumnId(0), name: "value".into(), data_type: logical,
            qualifier: None, qualified_only: false, unnamed: false,
        }]).unwrap();
        let document = source.apply_document(&[0], &schema, &memory).unwrap();
        drop(source);
        let [Column::Arrow { array, .. }] = document.columns.as_slice() else { panic!("Expected a document array"); };
        let array = array.as_any().downcast_ref::<StructArray>().unwrap();
        let child = array.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(child.values().as_ptr(), values_ptr);
        assert_eq!(child.iter().collect::<Vec<_>>(), [Some(17), None, Some(19)]);
        assert!(source_memory.used_bytes() > 0);
        drop(document);
        assert_eq!(source_memory.used_bytes(), 0);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn dependent_columns_share_page_spans_across_slicing_and_source_drop() {
        let memory = MemoryBudget::new(1 << 20);
        let logical = LogicalType::Scalar(ScalarType::I64);
        let field = Arc::new(rtdb_columnar::to_field("value", &logical).unwrap());
        let page = |values: Vec<i64>| {
            let len = values.len();
            let array = Arc::new(Int64Array::from(values));
            let columns = (0..2)
                .map(|_| Column::Arrow {
                    array: array.clone(),
                    field: Arc::clone(&field),
                    logical: Some(logical.clone()),
                })
                .collect();
            ColumnBatch::settled(columns, len, memory.reserve(4096).unwrap()).unwrap()
        };
        let mut pages = ApplyPages::new(&memory).unwrap();
        pages.push(0, Some(page(vec![11, 12]))).unwrap();
        pages.push(1, None).unwrap();
        pages.push(2, Some(page(vec![21, 22]))).unwrap();
        let (first, first_charge) = pages.dependent_column(0, &memory).unwrap();
        let (second, second_charge) = pages.dependent_column(1, &memory).unwrap();
        let Column::Selected(first) = first else { panic!("Expected a dependent selection") };
        let Column::Selected(mut second) = second else { panic!("Expected a dependent selection") };
        let (SelectedRows::Apply(first_rows), SelectedRows::Apply(second_rows)) = (&first.cells, &second.cells) else {
            panic!("Expected the shared page spans")
        };
        assert!(Arc::ptr_eq(&first_rows.spans, &second_rows.spans));
        assert_eq!(first_rows.spans.spans.len(), 3);
        second.cells = second.cells.slice(1..4);
        drop(pages);
        let second = Column::Selected(second);
        let values = (0..3).map(|row| second.value(row).unwrap()).collect::<Vec<_>>();
        assert_eq!(values, [Value::I64(12), Value::Null, Value::I64(21)]);
        drop((first, first_charge, second, second_charge));
        assert_eq!(memory.used_bytes(), 0);
    }
}
