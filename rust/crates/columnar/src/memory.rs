use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, UInt32Array, make_array};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_data::ArrayData;
use rtdb_types::{Anomaly, Result};

#[derive(Debug)]
pub struct MemoryBudget {
    limit_bytes: usize,
    used_bytes: AtomicUsize,
    peak_bytes: AtomicUsize,
    parent: Option<Arc<MemoryBudget>>,
}

impl MemoryBudget {
    pub fn new(limit_bytes: usize) -> Arc<Self> {
        Arc::new(Self { limit_bytes, used_bytes: AtomicUsize::new(0), peak_bytes: AtomicUsize::new(0), parent: None })
    }

    /// Tracks a region's retained and peak bytes while charging every reservation to the shared parent limit.
    /// Retained reservations keep both budgets alive after the region closes.
    pub fn scope(self: &Arc<Self>) -> Arc<Self> {
        Arc::new(Self { limit_bytes: self.limit_bytes, used_bytes: AtomicUsize::new(0), peak_bytes: AtomicUsize::new(0), parent: Some(Arc::clone(self)) })
    }

    /// Reserves bytes without waiting; release occurs when the last reservation handle drops.
    pub fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Arc<MemoryReservation>> {
        let parent = self.parent.as_ref().map(|parent| parent.reserve(bytes)).transpose()?;
        let mut current = self.used_bytes.load(Ordering::Relaxed);
        loop {
            let next = current.checked_add(bytes).filter(|next| *next <= self.limit_bytes)
                .ok_or_else(|| Anomaly::incorrect("rtdb/memory-budget-exceeded", format!("Cannot reserve {bytes} bytes from budget {} with {current} retained", self.limit_bytes)))?;
            match self.used_bytes.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => {
                    self.peak_bytes.fetch_max(next, Ordering::Relaxed);
                    return Ok(Arc::new(MemoryReservation { budget: Arc::clone(self), bytes, parent }));
                }
                Err(actual) => current = actual,
            }
        }
    }

    pub fn used_bytes(&self) -> usize { self.used_bytes.load(Ordering::Acquire) }
    pub fn peak_bytes(&self) -> usize { self.peak_bytes.load(Ordering::Relaxed) }
    pub fn limit_bytes(&self) -> usize { self.limit_bytes }
}

#[derive(Debug)]
pub struct MemoryReservation {
    budget: Arc<MemoryBudget>,
    bytes: usize,
    parent: Option<Arc<MemoryReservation>>,
}

impl MemoryReservation {
    pub fn bytes(&self) -> usize { self.bytes }
    pub fn budget(&self) -> &Arc<MemoryBudget> { &self.budget }

    fn shrink_to(&mut self, bytes: usize) {
        if let Some(parent) = &mut self.parent { Arc::get_mut(parent).expect("reservation exclusively owns its parent lease").shrink_to(bytes); }
        let released = self.bytes - bytes;
        self.bytes = bytes;
        self.budget.used_bytes.fetch_sub(released, Ordering::AcqRel);
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) { self.budget.used_bytes.fetch_sub(self.bytes, Ordering::AcqRel); }
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
        if batch.get_array_memory_size() > reservation.bytes() {
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
                Arc::get_mut(&mut reservation).ok_or_else(|| Anomaly::fault("rtdb/shared-allocation-reservation", "Selection reservation escaped before allocation completed"))?.shrink_to(bytes);
                Self::with_reservation(batch, reservation)
            }
        }
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
    pub(crate) fn into_parts(self) -> (ArrayRef, Arc<MemoryReservation>) { (self.array, self.reservation) }
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
    _reservation: Arc<MemoryReservation>,
}

fn attach_buffer(buffer: &Buffer, reservation: &Arc<MemoryReservation>) -> Result<Buffer> {
    let offset = buffer.ptr_offset();
    let span = offset.checked_add(buffer.len()).ok_or_else(|| Anomaly::fault("rtdb/buffer-span-overflow", "Arrow buffer span exceeds usize"))?;
    let owner = Arc::new(RetainedAllocation { _buffer: buffer.clone(), _reservation: Arc::clone(reservation) });
    // SAFETY: the owner retains the original immutable allocation; its base pointer covers this existing slice's offset and length.
    let retained = unsafe { Buffer::from_custom_allocation(buffer.data_ptr(), span, owner) };
    Ok(retained.slice_with_length(offset, buffer.len()))
}

fn attach_data(data: ArrayData, reservation: &Arc<MemoryReservation>) -> Result<ArrayData> {
    let buffers = data.buffers().iter().map(|buffer| attach_buffer(buffer, reservation)).collect::<Result<Vec<_>>>()?;
    let children = data.child_data().iter().map(|child| attach_data(child.clone(), reservation)).collect::<Result<Vec<_>>>()?;
    let nulls = data.nulls().map(|nulls| {
        let buffer = attach_buffer(nulls.buffer(), reservation)?;
        Ok::<NullBuffer, Anomaly>(NullBuffer::new(BooleanBuffer::new(buffer, nulls.offset(), nulls.len())))
    }).transpose()?;
    data.into_builder().buffers(buffers).child_data(children).nulls(nulls).build().map_err(super::arrow_error)
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
