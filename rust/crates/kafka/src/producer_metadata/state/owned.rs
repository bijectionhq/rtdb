use std::{mem::size_of, ops::Deref, sync::{Arc, OnceLock}};
use rtdb_types::{Anomaly, MemoryBudget, MemoryReservation, Result, java_hash_map::Admission};
use super::super::text::JavaText;

pub(super) type Lease = Arc<MemoryReservation>;

pub(super) struct Budget(pub Arc<MemoryBudget>);
impl Admission for Budget {
    type Lease = Lease;
    fn reserve(&mut self, bytes: usize) -> Result<Lease> {
        let control = size_of::<MemoryReservation>().checked_add(4 * size_of::<usize>()).ok_or_else(overflow)?;
        let controls = self.0.reservation_lease_count()?.checked_mul(control).ok_or_else(overflow)?;
        self.0.reserve(bytes.checked_add(controls).ok_or_else(overflow)?)
    }
}

pub(super) fn overflow() -> Anomaly {
    Anomaly::incorrect("rtdb/allocation-overflow", "Producer metadata allocation size overflow")
}
pub(super) fn allocation_failed() -> Anomaly {
    Anomaly::incorrect("rtdb/allocation-failed", "Producer metadata allocation failed")
}
pub(super) fn bytes<T>(count: usize) -> Result<usize> { count.checked_mul(size_of::<T>()).ok_or_else(overflow) }

struct Payload<T> { value: T, _lease: Lease }
pub(super) struct Shared<T>(Arc<Payload<T>>);
impl<T> Clone for Shared<T> { fn clone(&self) -> Self { Self(Arc::clone(&self.0)) } }
impl<T> Deref for Shared<T> { type Target = T; fn deref(&self) -> &T { &self.0.value } }
impl<T> Shared<T> {
    pub(super) fn new(value: T, budget: &mut Budget) -> Result<Self> {
        let size = size_of::<Payload<T>>().checked_add(2 * size_of::<usize>()).ok_or_else(overflow)?;
        let lease = budget.reserve(size)?;
        Ok(Self(Arc::new(Payload { value, _lease: lease })))
    }
    pub(super) fn same_object(&self, other: &Self) -> bool { Arc::ptr_eq(&self.0, &other.0) }
}

enum Storage<T> { Empty, Allocated { values: Vec<T>, _lease: Lease } }
pub(super) struct Vector<T>(Storage<T>);
impl<T> Vector<T> {
    pub(super) fn new() -> Self { Self(Storage::Empty) }
    pub(super) fn with_capacity(capacity: usize, budget: &mut Budget) -> Result<Self> {
        if capacity == 0 { return Ok(Self::new()); }
        let lease = budget.reserve(bytes::<T>(capacity)?)?;
        let mut values = Vec::new();
        values.try_reserve_exact(capacity).map_err(|_| allocation_failed())?;
        Ok(Self(Storage::Allocated { values, _lease: lease }))
    }
    pub(super) fn as_slice(&self) -> &[T] {
        match &self.0 { Storage::Empty => &[], Storage::Allocated { values, .. } => values }
    }
    pub(super) fn as_mut_slice(&mut self) -> &mut [T] {
        match &mut self.0 { Storage::Empty => &mut [], Storage::Allocated { values, .. } => values }
    }
    pub(super) fn len(&self) -> usize { self.as_slice().len() }
    pub(super) fn push(&mut self, value: T, budget: &mut Budget) -> Result<()> {
        let capacity = match &self.0 { Storage::Empty => 0, Storage::Allocated { values, .. } => values.capacity() };
        if self.len() == capacity {
            let next = if capacity == 0 { 4 } else { capacity.checked_mul(2).ok_or_else(overflow)? };
            let mut replacement = Self::with_capacity(next, budget)?;
            if let Storage::Allocated { values: destination, .. } = &mut replacement.0 {
                if let Storage::Allocated { values: source, .. } = &mut self.0 { destination.append(source); }
            }
            *self = replacement;
        }
        match &mut self.0 { Storage::Allocated { values, .. } => { values.push(value); Ok(()) }, Storage::Empty => unreachable!() }
    }
}

/// An admitted immutable Java string decoded from original UTF8 wire bytes.
/// Clones share its bytes and reservation through final release.
#[derive(Clone)]
pub struct Text { value: Shared<TextValue> }
struct TextValue { bytes: Vector<u8>, hash: i32, identity_hash: OnceLock<i32> }
impl Text {
    pub(super) fn from_raw(raw: &[u8], budget: &mut Budget) -> Result<Self> {
        let text = JavaText::new(raw);
        let length = text.utf8_len().map_err(|_| overflow())?;
        let mut bytes = Vector::with_capacity(length, budget)?;
        if let Storage::Allocated { values, .. } = &mut bytes.0 {
            values.resize(length, 0);
            text.write_utf8(values).map_err(|_| overflow())?;
        }
        Ok(Self { value: Shared::new(TextValue { bytes, hash: text.hash_code(), identity_hash: OnceLock::new() }, budget)? })
    }
    /// Borrows the Java replacement-decoded value without allocating.
    pub fn as_str(&self) -> &str { std::str::from_utf8(self.value.bytes.as_slice()).expect("Java text decoder produces UTF8") }
    pub(super) fn hash(&self) -> i32 { self.value.hash }
    /// Compares retained string object identity, independently of decoded equality.
    pub fn same_object(&self, other: &Self) -> bool { self.value.same_object(&other.value) }
    pub(super) fn identity_hash(&self) -> &OnceLock<i32> { &self.value.identity_hash }
}
impl PartialEq for Text { fn eq(&self, other: &Self) -> bool { self.as_str() == other.as_str() } }
impl Eq for Text {}
impl std::fmt::Debug for Text { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.as_str().fmt(f) } }

pub(super) fn array(values: impl ExactSizeIterator<Item = i32>, budget: &mut Budget) -> Result<Shared<Vector<i32>>> {
    let mut array = Vector::with_capacity(values.len(), budget)?;
    for value in values { array.push(value, budget)?; }
    Shared::new(array, budget)
}
