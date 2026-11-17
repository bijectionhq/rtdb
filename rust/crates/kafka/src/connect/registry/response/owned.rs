use std::{alloc::Layout, mem::size_of, sync::Arc};
use rtdb_types::MemoryReservation;
use super::{ConversionContext, Result, allocation_failed, overflow};

pub(in crate::connect::registry) struct Values<T> { values: Vec<T>, _storage: Option<Arc<MemoryReservation>> }
impl<T> Values<T> {
    pub(in crate::connect::registry) fn new() -> Self { Self { values: Vec::new(), _storage: None } }
    pub(in crate::connect::registry) fn with_capacity(capacity: usize, context: &mut ConversionContext) -> Result<Self> {
        if capacity == 0 { return Ok(Self::new()); }
        Layout::array::<T>(capacity).map_err(|_| overflow())?;
        let bytes = capacity.checked_mul(size_of::<T>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?;
        let storage = context.reserve(bytes)?;
        let mut values = Vec::new();
        values.try_reserve_exact(capacity).map_err(|_| allocation_failed())?;
        Ok(Self { values, _storage: Some(storage) })
    }
    pub(in crate::connect::registry) fn as_slice(&self) -> &[T] { &self.values }
    pub(in crate::connect::registry) fn as_mut_slice(&mut self) -> &mut [T] { &mut self.values }
    pub(in crate::connect::registry) fn len(&self) -> usize { self.values.len() }
    pub(in crate::connect::registry) fn push(&mut self, value: T, context: &mut ConversionContext) -> Result<()> {
        if self.values.len() == self.values.capacity() {
            let capacity = if self.values.capacity() == 0 { 4 } else { self.values.capacity().checked_mul(2).ok_or_else(overflow)? };
            let mut replacement = Self::with_capacity(capacity, context)?;
            replacement.values.append(&mut self.values);
            *self = replacement;
        }
        self.values.push(value);
        Ok(())
    }
    pub(in crate::connect::registry) fn pop(&mut self) -> Option<T> { self.values.pop() }
}
impl<T> Default for Values<T> { fn default() -> Self { Self::new() } }
