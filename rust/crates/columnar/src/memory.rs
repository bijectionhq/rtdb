use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, UInt32Array, make_array};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_data::ArrayData;
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

    pub fn column(&mut self, temporary: usize, build: impl FnOnce() -> Result<ArrayRef>) -> Result<ArrayRef> {
        let reservation = Arc::get_mut(&mut self.reservation).expect("batch construction owns its reservation");
        let retained = reservation.bytes();
        reservation.grow_to(retained.checked_add(temporary).ok_or_else(allocation_overflow)?)?;
        let array = build()?;
        reservation.shrink_to(retained.checked_add(array.get_array_memory_size()).ok_or_else(allocation_overflow)?)?;
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
}

impl OwnedBatch {
    /// Adopts an existing allocation and charges its Arrow memory size to the budget.
    /// Callers allocating large batches should reserve before allocation and use `with_reservation`.
    pub fn new(batch: RecordBatch, budget: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = budget.reserve(batch.get_array_memory_size())?;
        Self::with_reservation(batch, reservation)
    }

    /// The reservation must cover all buffers retained by this batch.
    pub fn with_reservation(batch: RecordBatch, reservation: Arc<MemoryReservation>) -> Result<Self> {
        if batch.get_array_memory_size() > reservation.bytes() && shared_allocation_size(&batch)? > reservation.bytes() {
            return Err(Anomaly::incorrect("rtdb/undersized-reservation", "Batch allocation exceeds its reservation"));
        }
        let arrays = batch.columns().iter().map(|array| {
            attach_data(array.to_data(), &reservation).map(make_array)
        }).collect::<Result<Vec<_>>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        let batch = RecordBatch::try_new_with_options(batch.schema(), arrays, &options).map_err(super::arrow_error)?;
        Ok(Self { batch, reservation })
    }

    /// Cloned Arrow arrays retain the reservation through their allocation owners.
    pub fn batch(&self) -> &RecordBatch { &self.batch }
    pub fn reservation(&self) -> &Arc<MemoryReservation> { &self.reservation }

    pub fn with_appended_column(&self, field: arrow_schema::Field, column: arrow_array::ArrayRef,
        reservation: Arc<MemoryReservation>) -> Result<Self> {
        if column.get_array_memory_size() > reservation.bytes() {
            return Err(Anomaly::incorrect("rtdb/undersized-reservation", "Appended column exceeds its reservation"));
        }
        let schema = self.batch.schema();
        let mut fields = schema.fields().to_vec();
        fields.push(Arc::new(field));
        let mut columns = self.batch.columns().to_vec();
        columns.push(make_array(attach_data(column.to_data(), &reservation)?));
        let schema = Arc::new(arrow_schema::Schema::new_with_metadata(fields, schema.metadata().clone()));
        let batch = RecordBatch::try_new(schema, columns).map_err(super::arrow_error)?;
        Ok(Self { batch, reservation: Arc::clone(&self.reservation) })
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
        Ok(Self { batch: self.batch.slice(offset, length), reservation: Arc::clone(&self.reservation) })
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
                Self::with_reservation(batch, reservation)
            }
        }
    }
}

/// Counts each buffer base address once, using the largest capacity reported for it.
pub fn shared_allocation_size(batch: &RecordBatch) -> Result<usize> {
    let mut buffers = BTreeMap::new();
    let mut descriptors = std::mem::size_of::<RecordBatch>();
    for array in batch.columns() {
        descriptors = descriptors.checked_add(array.get_array_memory_size().checked_sub(array.get_buffer_memory_size()).ok_or_else(allocation_overflow)?)
            .ok_or_else(allocation_overflow)?;
        collect_buffers(&array.to_data(), &mut buffers);
    }
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

fn attach_data(data: ArrayData, reservation: &Arc<MemoryReservation>) -> Result<ArrayData> {
    let buffers = data.buffers().iter().map(|buffer| retain_buffer(buffer, reservation)).collect::<Vec<_>>();
    let children = data.child_data().iter().map(|child| attach_data(child.clone(), reservation)).collect::<Result<Vec<_>>>()?;
    let nulls = data.nulls().map(|nulls| {
        let buffer = retain_buffer(nulls.buffer(), reservation);
        NullBuffer::new(BooleanBuffer::new(buffer, nulls.offset(), nulls.len()))
    });
    data.into_builder().buffers(buffers).child_data(children).nulls(nulls).build().map_err(super::arrow_error)
}

/// Keeps a copy's allocation charge with its Arrow buffers through moves and slices.
pub fn retain_array(array: ArrayRef, reservation: &Arc<MemoryReservation>) -> Result<ArrayRef> {
    attach_data(array.to_data(), reservation).map(make_array)
}

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
