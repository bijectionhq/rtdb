use std::{collections::{BTreeMap, BTreeSet}, ops::Range, sync::Arc};

use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, UInt32Array, make_array};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_data::ArrayData;
use arrow_schema::DataType;
use rtdb_types::{Anomaly, Result};

pub use rtdb_types::{MemoryBudget, MemoryReservation};

/// Holds the retained arrays of one batch while admitting only one column's construction scratch.
pub struct BatchBuilder { reservation: Arc<MemoryReservation> }

impl BatchBuilder {
    pub fn new(metadata: usize, memory: &Arc<MemoryBudget>) -> Result<Self> {
        Ok(Self { reservation: memory.reserve(metadata)? })
    }

    pub fn reserve_scratch(&self, bytes: usize) -> Result<Arc<MemoryReservation>> {
        self.reservation.budget().reserve(bytes)
    }

    /// Moves the charge for metadata discovered during column decoding to the batch.
    pub fn retain_metadata(&mut self, scratch: &mut MemoryReservation, bytes: usize) -> Result<()> {
        if !Arc::ptr_eq(self.reservation.budget(), scratch.budget()) {
            return Err(Anomaly::incorrect("rtdb/reservation-budget", "Reservations belong to different budgets"));
        }
        let mut metadata = scratch.split_off(bytes)?;
        Arc::get_mut(&mut self.reservation).expect("batch construction owns its reservation")
            .absorb(Arc::get_mut(&mut metadata).expect("metadata charge has a single owner"))
    }

    pub fn column(&mut self, temporary: usize, build: impl FnOnce() -> Result<ArrayRef>) -> Result<ArrayRef> {
        self.try_column(temporary, || build().map(Some)).map(|array| array.expect("Column construction returned an array"))
    }

    pub fn try_column(&mut self, temporary: usize, build: impl FnOnce() -> Result<Option<ArrayRef>>) -> Result<Option<ArrayRef>> {
        let reservation = Arc::get_mut(&mut self.reservation).expect("batch construction owns its reservation");
        let retained = reservation.bytes();
        reservation.grow_to(retained.checked_add(temporary).ok_or_else(allocation_overflow)?)?;
        let array = build()?;
        let added = array.as_ref().map_or(0, |array| array.get_array_memory_size());
        reservation.shrink_to(retained.checked_add(added).ok_or_else(allocation_overflow)?)?;
        Ok(array)
    }

    pub fn finish(self, batch: RecordBatch) -> Result<OwnedBatch> {
        OwnedBatch::with_reservation(batch, self.reservation)
    }

    pub fn finish_with_metadata(mut self, batch: RecordBatch, metadata: usize) -> Result<OwnedBatch> {
        let retained = batch.get_array_memory_size().checked_add(metadata).ok_or_else(allocation_overflow)?;
        Arc::get_mut(&mut self.reservation).expect("batch construction owns its reservation").shrink_to(retained)?;
        self.finish(batch)
    }
}

#[derive(Clone, Debug)]
pub struct OwnedBatch {
    batch: RecordBatch,
    reservation: Arc<MemoryReservation>,
    schema_reservation: Option<Arc<MemoryReservation>>,
    covers_buffer_backing: bool,
}

impl OwnedBatch {
    /// Adopts an existing allocation and charges its Arrow memory size to the budget.
    /// Callers allocating large batches should reserve before allocation and use `with_reservation`.
    pub fn new(batch: RecordBatch, budget: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = budget.reserve(batch.get_array_memory_size())?;
        let mut owned = Self::with_reservation(batch, reservation)?;
        // A foreign or previously wrapped array can report only its visible buffer size.
        owned.covers_buffer_backing = false;
        Ok(owned)
    }

    /// The reservation must cover all buffers retained by this batch.
    pub fn with_reservation(batch: RecordBatch, reservation: Arc<MemoryReservation>) -> Result<Self> {
        if batch.get_array_memory_size() > reservation.bytes() && shared_allocation_size(&batch)? > reservation.bytes() {
            return Err(Anomaly::incorrect("rtdb/undersized-reservation", "Batch allocation exceeds its reservation"));
        }
        let batch = attach_batch(batch, |buffer| retain_buffer(buffer, &reservation))?;
        Ok(Self { batch, reservation, schema_reservation: None, covers_buffer_backing: true })
    }

    /// Adopts a batch under its decode grant, leaving already funded backing with its existing owners.
    pub fn with_shared_buffers(batch: RecordBatch, grant: &mut MemoryReservation,
        schema_reservation: Arc<MemoryReservation>, is_funded: impl Fn(BufferIdentity) -> bool,
    ) -> Result<Self> {
        let mut allocations = BTreeMap::new();
        let mut bytes = std::mem::size_of::<RecordBatch>();
        for array in batch.columns() {
            bytes = sum(bytes, array_descriptors(array)?)?;
            visit_buffers(&array.to_data(), &mut |buffer| {
                count_new_buffer(&mut allocations, buffer, &is_funded)
            })?;
        }
        let bytes = allocations.into_values().try_fold(bytes, sum)?;
        let reservation = grant.split_off(bytes)?;
        let batch = attach_batch(batch, |buffer| {
            if is_funded(BufferIdentity::of(buffer)) { buffer.clone() }
            else { retain_buffer(buffer, &reservation) }
        })?;
        Ok(Self { batch, reservation, schema_reservation: Some(schema_reservation), covers_buffer_backing: false })
    }

    /// Adopts a decoded page, charging its slot and descriptors separately from each new backing allocation.
    /// Shared buffers carry their existing backing charge through the page and any independently retained slices.
    /// The funding predicate must select only buffers whose owners carry that charge.
    pub fn with_independent_buffers(batch: RecordBatch, grant: &mut MemoryReservation,
        schema_reservation: Arc<MemoryReservation>, is_funded: impl Fn(BufferIdentity) -> bool,
    ) -> Result<Self> {
        let mut allocations = BTreeMap::new();
        let mut descriptors = sum(256, std::mem::size_of::<Self>())?;
        for array in batch.columns() {
            descriptors = sum(descriptors, sum(32, array_descriptors(array)?)?)?;
            visit_buffers(&array.to_data(), &mut |buffer| {
                descriptors = sum(descriptors, 256)?;
                count_new_buffer(&mut allocations, buffer, &is_funded)
            })?;
        }
        let reservation = grant.split_off(descriptors)?;
        let allocations = split_buffer_reservations(allocations, grant)?;
        let descriptors: Arc<dyn Send + Sync> = Arc::new((Arc::clone(&reservation), Arc::clone(&schema_reservation)));
        let batch = attach_batch(batch, |buffer| {
            let owner: Arc<dyn Send + Sync> = match allocations.get(&BufferIdentity::of(buffer)) {
                Some(backing) => Arc::new((Arc::clone(backing), Arc::clone(&descriptors))),
                None => Arc::clone(&descriptors),
            };
            retain_buffer_with_owner(buffer, owner)
        })?;
        Ok(Self { batch, reservation, schema_reservation: Some(schema_reservation), covers_buffer_backing: false })
    }

    /// Carries the owners through a layout change with already funded backing buffers.
    pub fn with_retained_buffers(&self, batch: RecordBatch) -> Self {
        Self { batch, reservation: Arc::clone(&self.reservation), schema_reservation: self.schema_reservation.clone(), covers_buffer_backing: false }
    }

    /// The reservation covers the batch's descriptors, including arrays without backing buffers.
    /// Each column with backing buffers already carries its allocation owners.
    pub fn with_funded_columns(batch: RecordBatch, reservation: Arc<MemoryReservation>) -> Self {
        Self { batch, reservation, schema_reservation: None, covers_buffer_backing: false }
    }

    /// The reservation covers the allocation of this batch's immutable schema.
    pub fn with_schema_reservation(mut self, reservation: Arc<MemoryReservation>) -> Self {
        self.schema_reservation = Some(reservation);
        self
    }

    /// Cloned Arrow arrays retain the reservation through their allocation owners.
    pub fn batch(&self) -> &RecordBatch { &self.batch }
    /// The batch's own charge. Shared buffers and the schema can carry separate charges.
    pub fn reservation(&self) -> &Arc<MemoryReservation> { &self.reservation }

    /// Bounds backing held by a cloned Arrow buffer, including bytes hidden by wrapped slices.
    pub fn buffer_backing(&self) -> Option<&Arc<MemoryReservation>> {
        (self.covers_buffer_backing && self.reservation.backing().is_empty()).then_some(&self.reservation)
    }

    pub fn owns_schema(&self) -> bool { self.schema_reservation.is_some() }

    pub fn with_appended_column(&self, field: arrow_schema::Field, column: arrow_array::ArrayRef,
        reservation: Arc<MemoryReservation>) -> Result<Self> {
        self.with_inserted_column(self.batch.num_columns(), field, column, reservation)
    }

    pub fn with_inserted_column(&self, index: usize, field: arrow_schema::Field,
        column: arrow_array::ArrayRef, reservation: Arc<MemoryReservation>) -> Result<Self> {
        if index > self.batch.num_columns() || column.get_array_memory_size() > reservation.bytes() {
            return Err(Anomaly::incorrect("rtdb/batch-column", "Inserted column is outside its batch or reservation"));
        }
        let schema = self.batch.schema();
        let mut fields = schema.fields().to_vec();
        fields.insert(index, Arc::new(field));
        let mut columns = self.batch.columns().to_vec();
        columns.insert(index, make_array(attach_data(column.to_data(), &reservation)?));
        let schema = Arc::new(arrow_schema::Schema::new_with_metadata(fields, schema.metadata().clone()));
        let batch = RecordBatch::try_new(schema, columns).map_err(super::arrow_error)?;
        Ok(self.with_retained_buffers(batch))
    }

    pub fn with_replaced_column(&self, index: usize, column: arrow_array::ArrayRef,
        reservation: Arc<MemoryReservation>) -> Result<Self> {
        if index >= self.batch.num_columns() || column.get_array_memory_size() > reservation.bytes() {
            return Err(Anomaly::incorrect("rtdb/batch-column", "Replacement column is outside its batch or reservation"));
        }
        let mut columns = self.batch.columns().to_vec();
        columns[index] = make_array(attach_data(column.to_data(), &reservation)?);
        let batch = RecordBatch::try_new(self.batch.schema(), columns).map_err(super::arrow_error)?;
        Ok(self.with_retained_buffers(batch))
    }

    /// Copies distinct rows from batches with the same physical schema, in the specified order.
    pub fn interleave_distinct_rows(
        sources: &[&Self], rows: &[(usize, usize)], memory: &Arc<MemoryBudget>,
    ) -> Result<Self> {
        let first = sources.first().ok_or_else(|| Anomaly::incorrect("rtdb/interleave", "Row selection has no input batch"))?;
        let schema = first.batch.schema();
        let base = rows.len().checked_mul(128).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(allocation_overflow)?;
        let bound = sources.iter().try_fold(base, |bytes, source| {
            if source.batch.schema() != schema { return Err(Anomaly::incorrect("rtdb/interleave", "Input batch schemas differ")); }
            source.batch.get_array_memory_size().checked_mul(4)
                .and_then(|size| size.checked_add(bytes)).ok_or_else(allocation_overflow)
        })?;
        let mut reservation = memory.reserve(bound)?;
        let mut seen = BTreeSet::new();
        for &(batch, row) in rows {
            if sources.get(batch).is_none_or(|source| row >= source.batch.num_rows()) || !seen.insert((batch, row)) {
                return Err(Anomaly::incorrect("rtdb/interleave", "Row selection contains an invalid or repeated position"));
            }
        }
        drop(seen);
        let batches = sources.iter().map(|source| &source.batch).collect::<Vec<_>>();
        let batch = arrow_select::interleave::interleave_record_batch(&batches, rows).map_err(super::arrow_error)?;
        let size = batch.get_array_memory_size().checked_add(4096).ok_or_else(allocation_overflow)?;
        // Interleaving a dense union can retain more than its sources' selected rows; charge what the copy holds.
        let reserved = Arc::get_mut(&mut reservation).expect("row copying owns its reservation");
        if size > reserved.bytes() { reserved.grow_to(size)?; } else { reserved.shrink_to(size)?; }
        Self::with_reservation(batch, reservation)
    }

    pub fn column(&self, index: usize) -> Result<OwnedColumn> {
        let array = self.batch.columns().get(index).ok_or_else(|| Anomaly::incorrect("rtdb/column-index", "Column index exceeds batch width"))?;
        Ok(OwnedColumn { array: Arc::clone(array), reservation: Arc::clone(&self.reservation) })
    }

    pub fn slice(&self, offset: usize, length: usize) -> Result<Self> {
        check_slice(offset, length, self.batch.num_rows())?;
        Ok(Self { batch: self.batch.slice(offset, length), ..self.clone() })
    }

    pub fn select(&self, selection: &Selection) -> Result<Self> {
        selection.validate(self.batch.num_rows())?;
        match selection {
            Selection::All => Ok(self.clone()),
            Selection::Range { offset, length } => self.slice(*offset, *length),
            Selection::Indices(indices) => {
                let bound = selection_bound(&self.batch, indices)?;
                let mut reservation = self.reservation.budget().reserve(bound)?;
                let indices = UInt32Array::from(indices.clone());
                let columns = self.batch.columns().iter().map(|array| {
                    arrow_select::take::take(array.as_ref(), &indices, None).map_err(super::arrow_error)
                }).collect::<Result<Vec<_>>>()?;
                let options = RecordBatchOptions::new().with_row_count(Some(indices.len()));
                let batch = RecordBatch::try_new_with_options(self.batch.schema(), columns, &options).map_err(super::arrow_error)?;
                let bytes = batch.get_array_memory_size();
                if bytes > reservation.bytes() {
                    return Err(Anomaly::fault("rtdb/selection-allocation-bound", "Arrow selection exceeded its reserved allocation bound"));
                }
                Arc::get_mut(&mut reservation).ok_or_else(|| Anomaly::fault("rtdb/shared-allocation-reservation", "Selection reservation escaped before allocation completed"))?.shrink_to(bytes)?;
                let mut owned = Self::with_reservation(batch, reservation)?;
                owned.schema_reservation = self.schema_reservation.clone();
                Ok(owned)
            }
        }
    }
}

/// Counts each buffer base address once, using the largest capacity reported for it.
pub fn shared_allocation_size(batch: &RecordBatch) -> Result<usize> {
    allocation_size(std::slice::from_ref(batch), None)
}

/// Counts retained batch allocations apart from the backing charged by `buffer`'s owner.
pub fn shared_allocation_size_excluding(batch: &RecordBatch, buffer: &Buffer) -> Result<usize> {
    allocation_size(std::slice::from_ref(batch), Some(buffer.data_ptr().as_ptr() as usize))
}

/// Counts shared buffer capacity once across all retained batches.
pub fn shared_batches_allocation_size(batches: &[RecordBatch]) -> Result<usize> {
    allocation_size(batches, None)
}

fn allocation_size(batches: &[RecordBatch], excluded: Option<usize>) -> Result<usize> {
    let mut buffers = BTreeMap::new();
    let mut descriptors = std::mem::size_of_val(batches);
    for batch in batches {
        for array in batch.columns() {
            descriptors = descriptors.checked_add(array.get_array_memory_size().checked_sub(array.get_buffer_memory_size()).ok_or_else(allocation_overflow)?)
                .ok_or_else(allocation_overflow)?;
            collect_buffers(&array.to_data(), &mut buffers);
        }
    }
    if let Some(base) = excluded { buffers.remove(&base); }
    buffers.values().try_fold(descriptors, |size, capacity| size.checked_add(*capacity).ok_or_else(allocation_overflow))
}

fn collect_buffers(data: &ArrayData, buffers: &mut BTreeMap<usize, usize>) {
    for buffer in data.buffers().iter().chain(data.nulls().map(NullBuffer::buffer)) {
        buffers.entry(buffer.data_ptr().as_ptr() as usize)
            .and_modify(|capacity| *capacity = (*capacity).max(buffer.capacity()))
            .or_insert(buffer.capacity());
    }
    for child in data.child_data() {
        collect_buffers(child, buffers);
    }
}

/// Returns a conservative count of distinct buffer bytes visible through the Arrow views.
/// Unsupported payloads receive zero credit, and out-of-bounds ranges return `None`.
pub fn visible_buffer_bytes(columns: &[ArrayRef]) -> Option<usize> {
    let mut ranges = Vec::new();
    for column in columns {
        let data = column.to_data();
        collect_visible_ranges(&data, 0..data.len(), &mut ranges)?;
    }
    ranges.sort_unstable_by_key(|range| range.start);
    let mut end = 0;
    let mut bytes = 0usize;
    for range in ranges {
        bytes = bytes.checked_add(range.end.saturating_sub(range.start.max(end)))?;
        end = end.max(range.end);
    }
    Some(bytes)
}

fn collect_visible_ranges(
    data: &ArrayData, rows: Range<usize>, ranges: &mut Vec<Range<usize>>,
) -> Option<()> {
    if rows.start > rows.end || rows.end > data.len() {
        return None;
    }
    if rows.is_empty() {
        return Some(());
    }

    let slots = offset_range(&rows, data.offset())?;
    let nulls = if let Some(nulls) = data.nulls() {
        if rows.end > nulls.len() {
            return None;
        }
        let bits = offset_range(&rows, nulls.offset())?;
        add_buffer_range(nulls.buffer(), bitmap_bytes(bits), ranges)?;
        (nulls.null_count() != 0)
            .then(|| nulls.slice(rows.start, rows.len()))
            .filter(|nulls| nulls.null_count() != 0)
    } else {
        None
    };

    match data.data_type() {
        DataType::Struct(_) => {
            // Arrow's StructArray::to_data has already applied the parent offset to its children.
            if data.offset() != 0 { return None; }
            if nulls.is_none() {
                for child in data.child_data() {
                    collect_visible_ranges(child, rows.clone(), ranges)?;
                }
            }
        }
        DataType::FixedSizeList(_, width) => {
            if nulls.is_none() {
                let children = scale_range(slots, usize::try_from(*width).ok()?)?;
                collect_visible_ranges(data.child_data().first()?, children, ranges)?;
            }
        }
        DataType::List(_) | DataType::Binary | DataType::Utf8 => {
            let offsets = data.buffers().first()?;
            let positions = slots.start..slots.end.checked_add(1)?;
            let offset_bytes = scale_range(positions.clone(), std::mem::size_of::<i32>())?;
            add_buffer_range(offsets, offset_bytes, ranges)?;
            let offsets = offsets.typed_data::<i32>().get(positions)?;
            // Arrow permits null slots to enclose payload bytes or child rows.
            if nulls.is_some_and(|nulls| offsets.windows(2).zip(nulls.iter()).any(|(pair, valid)| {
                !valid && pair[0] != pair[1]
            })) {
                return Some(());
            }
            let start = usize::try_from(*offsets.first()?).ok()?;
            let end = usize::try_from(*offsets.last()?).ok()?;
            let values = start..end;
            if matches!(data.data_type(), DataType::List(_)) {
                collect_visible_ranges(data.child_data().first()?, values, ranges)?;
            } else {
                add_buffer_range(data.buffers().get(1)?, values, ranges)?;
            }
        }
        DataType::Boolean => add_buffer_range(data.buffers().first()?, bitmap_bytes(slots), ranges)?,
        DataType::FixedSizeBinary(width) => {
            let values = scale_range(slots, usize::try_from(*width).ok()?)?;
            add_buffer_range(data.buffers().first()?, values, ranges)?;
        }
        ty => {
            if let Some(width) = ty.primitive_width() {
                add_buffer_range(data.buffers().first()?, scale_range(slots, width)?, ranges)?;
            }
        }
    }
    Some(())
}

fn offset_range(range: &Range<usize>, offset: usize) -> Option<Range<usize>> {
    Some(range.start.checked_add(offset)?..range.end.checked_add(offset)?)
}

fn scale_range(range: Range<usize>, width: usize) -> Option<Range<usize>> {
    Some(range.start.checked_mul(width)?..range.end.checked_mul(width)?)
}

fn bitmap_bytes(bits: Range<usize>) -> Range<usize> {
    bits.start / 8..bits.end.div_ceil(8)
}

fn add_buffer_range(buffer: &Buffer, bytes: Range<usize>, ranges: &mut Vec<Range<usize>>) -> Option<()> {
    if bytes.start > bytes.end || bytes.end > buffer.len() {
        return None;
    }
    if !bytes.is_empty() {
        ranges.push(offset_range(&bytes, buffer.as_ptr() as usize)?);
    }
    Some(())
}

#[derive(Clone, Debug)]
pub struct OwnedColumn {
    array: ArrayRef,
    reservation: Arc<MemoryReservation>,
}

impl OwnedColumn {
    /// Clones obtained through Arrow's array data retain the memory reservation.
    pub fn array(&self) -> &dyn Array { self.array.as_ref() }
    pub fn slice(&self, offset: usize, length: usize) -> Result<Self> {
        check_slice(offset, length, self.array.len())?;
        Ok(Self { array: self.array.slice(offset, length), reservation: Arc::clone(&self.reservation) })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Selection {
    All,
    Range { offset: usize, length: usize },
    Indices(Vec<u32>),
}

impl Selection {
    pub fn validate(&self, row_count: usize) -> Result<()> {
        match self {
            Self::All => Ok(()),
            Self::Range { offset, length } => check_slice(*offset, *length, row_count),
            Self::Indices(indices) if indices.iter().all(|index| (*index as usize) < row_count) => Ok(()),
            Self::Indices(_) => Err(Anomaly::incorrect("rtdb/row-index", "Selection index exceeds batch length")),
        }
    }
}

fn check_slice(offset: usize, length: usize, total: usize) -> Result<()> {
    if offset.checked_add(length).is_some_and(|end| end <= total) { Ok(()) }
    else { Err(Anomaly::incorrect("rtdb/slice-bounds", "Slice exceeds retained batch length")) }
}

struct RetainedAllocation {
    _buffer: Buffer,
    _owner: std::panic::AssertUnwindSafe<Arc<dyn Send + Sync>>,
}

/// Retains an existing allocation and its charge through this exact byte view.
pub fn retain_buffer(buffer: &Buffer, reservation: &Arc<MemoryReservation>) -> Buffer {
    retain_buffer_with_owner(buffer, reservation.clone())
}

/// Retains an exact byte view along with a memory charge and any required custody.
pub fn retain_buffer_with_owner(buffer: &Buffer, owner: Arc<dyn Send + Sync>) -> Buffer {
    let start = std::ptr::NonNull::from(buffer.as_slice()).cast();
    // The erased owner only holds the allocation charge or permit until drop.
    let owner = Arc::new(RetainedAllocation {
        _buffer: buffer.clone(), _owner: std::panic::AssertUnwindSafe(owner),
    });
    // SAFETY: the owner keeps the original immutable buffer alive for this slice's address and length.
    unsafe { Buffer::from_custom_allocation(start, buffer.len(), owner) }
}

fn attach_batch(batch: RecordBatch, own: impl Fn(&Buffer) -> Buffer) -> Result<RecordBatch> {
    let arrays = batch.columns().iter().map(|array| attach_buffers(array.to_data(), &own).map(make_array))
        .collect::<Result<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(batch.schema(), arrays, &options).map_err(super::arrow_error)
}

fn attach_buffers(data: ArrayData, own: &impl Fn(&Buffer) -> Buffer) -> Result<ArrayData> {
    let buffers = data.buffers().iter().map(own).collect::<Vec<_>>();
    let children = data.child_data().iter().map(|child| attach_buffers(child.clone(), own)).collect::<Result<Vec<_>>>()?;
    let nulls = data.nulls().map(|nulls| {
        let buffer = own(nulls.buffer());
        NullBuffer::new(BooleanBuffer::new(buffer, nulls.offset(), nulls.len()))
    });
    data.into_builder().buffers(buffers).child_data(children).nulls(nulls).build().map_err(super::arrow_error)
}

fn attach_data(data: ArrayData, reservation: &Arc<MemoryReservation>) -> Result<ArrayData> {
    attach_buffers(data, &|buffer| retain_buffer(buffer, reservation))
}

/// Keeps a copy's allocation charge with its Arrow buffers through moves and slices.
pub fn retain_array(array: ArrayRef, reservation: &Arc<MemoryReservation>) -> Result<ArrayRef> {
    attach_data(array.to_data(), reservation).map(make_array)
}

#[derive(Clone, Copy)]
pub enum ResultArrayFunding<'a> {
    Copy,
    Shared(&'a Arc<MemoryReservation>),
    Relayout(&'a ArrayRef, &'a Arc<MemoryReservation>),
}

/// Settles an exclusive construction grant, excluding backing covered by the source owner.
/// Arrow buffer clones carry their custody, while bufferless descriptors are charged to the page.
pub fn retain_result_array(
    array: ArrayRef, mut grant: Arc<MemoryReservation>, funding: ResultArrayFunding<'_>,
    page: &mut MemoryReservation, schema: &Arc<MemoryReservation>,
) -> Result<ArrayRef> {
    let mut source_buffers = BTreeSet::new();
    let source_charge = match funding {
        ResultArrayFunding::Copy => None,
        ResultArrayFunding::Shared(charge) => Some(charge),
        ResultArrayFunding::Relayout(source, charge) => {
            visit_buffers(&source.to_data(), &mut |buffer| {
                source_buffers.insert(BufferIdentity::of(buffer));
                Ok(())
            })?;
            Some(charge)
        }
    };
    let is_shared = |buffer| match funding {
        ResultArrayFunding::Shared(_) => true,
        ResultArrayFunding::Relayout(..) => source_buffers.contains(&buffer),
        ResultArrayFunding::Copy => false,
    };
    let data = array.to_data();
    let mut bytes = sum(256, array_descriptors(&array)?)?;
    let mut has_buffers = false;
    let mut new_buffers = BTreeMap::new();
    visit_buffers(&data, &mut |buffer| {
        has_buffers = true;
        if is_shared(BufferIdentity::of(buffer)) {
            bytes = sum(bytes, 128)?;
            Ok(())
        } else {
            count_new_buffer(&mut new_buffers, buffer, &|_| false)
        }
    })?;
    let bytes = new_buffers.into_values().try_fold(bytes, sum)?;
    let lease = Arc::get_mut(&mut grant).ok_or_else(|| {
        Anomaly::fault("rtdb/shared-allocation-reservation", "Result column construction grant escaped")
    })?;
    if bytes > lease.bytes() { lease.grow_to(bytes)?; }
    else { lease.shrink_to(bytes)?; }
    if !has_buffers {
        page.absorb(lease)?;
        return Ok(array);
    }
    drop(array);
    let output_owner: Arc<dyn Send + Sync> = Arc::new((grant, Arc::clone(schema)));
    let shared_owner: Option<Arc<dyn Send + Sync>> = source_charge.map(|charge| {
        Arc::new((Arc::clone(charge), Arc::clone(&output_owner))) as Arc<dyn Send + Sync>
    });
    let data = attach_buffers(data, &|buffer| {
        let owner = if is_shared(BufferIdentity::of(buffer)) {
            shared_owner.as_ref().expect("Shared result buffers have a source charge")
        } else {
            &output_owner
        };
        retain_buffer_with_owner(buffer, Arc::clone(owner))
    })?;
    Ok(make_array(data))
}

/// Identifies backing storage while its Arrow buffers remain live.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct BufferIdentity { base: usize, capacity: usize }

impl BufferIdentity {
    /// Identifies the allocation behind this view while the buffer is live.
    pub fn of(buffer: &Buffer) -> Self {
        Self { base: buffer.data_ptr().as_ptr() as usize, capacity: buffer.capacity() }
    }
}

/// An Arrow array with separately owned backing allocations that can be shared by other arrays.
pub struct RetainedArray {
    array: ArrayRef,
    buffers: BTreeSet<BufferIdentity>,
    descriptors: Arc<MemoryReservation>,
}

impl RetainedArray {
    pub fn new(array: ArrayRef, grant: &mut MemoryReservation, is_funded: impl Fn(BufferIdentity) -> bool) -> Result<Self> {
        let data = array.to_data();
        let mut allocations = BTreeMap::new();
        let mut descriptors = sum(256, array_descriptors(&array)?)?;
        visit_buffers(&data, &mut |buffer| {
            descriptors = sum(descriptors, 128)?;
            count_new_buffer(&mut allocations, buffer, &is_funded)
        })?;
        // One IPC body can back multiple slices, and a delta can keep only some of those slices.
        let descriptors = grant.split_off(descriptors)?;
        let allocations = split_buffer_reservations(allocations, grant)?;
        let data = attach_buffers(data, &|buffer| match allocations.get(&BufferIdentity::of(buffer)) {
            Some(reservation) => retain_buffer(buffer, reservation),
            None => buffer.clone(),
        })?;
        let mut buffers = BTreeSet::new();
        visit_buffers(&data, &mut |buffer| {
            buffers.insert(BufferIdentity::of(buffer));
            Ok(())
        })?;
        Ok(Self { array: make_array(data), buffers, descriptors })
    }

    pub fn array(&self) -> &ArrayRef { &self.array }
    pub fn funds(&self, buffer: BufferIdentity) -> bool { self.buffers.contains(&buffer) }
    pub fn descriptor_bytes(&self) -> usize { self.descriptors.bytes() }
}

fn split_buffer_reservations(allocations: BTreeMap<BufferIdentity, usize>, grant: &mut MemoryReservation)
    -> Result<BTreeMap<BufferIdentity, Arc<MemoryReservation>>> {
    allocations.into_iter().map(|(key, bytes)| Ok((key, grant.split_off(bytes)?))).collect()
}

fn visit_buffers(data: &ArrayData, visit: &mut impl FnMut(&Buffer) -> Result<()>) -> Result<()> {
    for buffer in data.buffers().iter().chain(data.nulls().map(NullBuffer::buffer)) { visit(buffer)?; }
    for child in data.child_data() { visit_buffers(child, visit)?; }
    Ok(())
}

fn count_new_buffer(allocations: &mut BTreeMap<BufferIdentity, usize>, buffer: &Buffer,
    is_funded: &impl Fn(BufferIdentity) -> bool,
) -> Result<()> {
    let identity = BufferIdentity::of(buffer);
    if !is_funded(identity) {
        let bytes = allocations.entry(identity).or_insert(identity.capacity);
        *bytes = sum(*bytes, 128)?;
    }
    Ok(())
}

fn array_descriptors(array: &ArrayRef) -> Result<usize> {
    array.get_array_memory_size().checked_sub(array.get_buffer_memory_size()).ok_or_else(allocation_overflow)
}

fn sum(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(allocation_overflow) }

fn selection_bound(batch: &RecordBatch, indices: &[u32]) -> Result<usize> {
    let mut bound = indices.len().checked_mul(std::mem::size_of::<u32>()).ok_or_else(allocation_overflow)?;
    for array in batch.columns() {
        let data = array.to_data();
        let overhead = allocation_overhead(&data)?;
        bound = bound.checked_add(overhead).ok_or_else(allocation_overflow)?;
        for index in indices {
            let slice = array.slice(*index as usize, 1).to_data();
            let bytes = slice.get_slice_memory_size().map_err(super::arrow_error)?;
            bound = bound.checked_add(bytes).and_then(|n| n.checked_add(overhead)).ok_or_else(allocation_overflow)?;
        }
    }
    bound.checked_mul(4).ok_or_else(allocation_overflow)
}

fn allocation_overhead(data: &ArrayData) -> Result<usize> {
    let mut size = 256usize.checked_add(std::mem::size_of::<ArrayData>()).ok_or_else(allocation_overflow)?;
    for child in data.child_data() { size = size.checked_add(allocation_overhead(child)?).ok_or_else(allocation_overflow)?; }
    Ok(size)
}

fn allocation_overflow() -> Anomaly { Anomaly::incorrect("rtdb/allocation-overflow", "Arrow allocation bound exceeds usize") }
