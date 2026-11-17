use super::*;

pub(in crate::cells) struct CapturedChildren<T> {
    values: Vec<T>,
    limit: usize,
    _reservation: Arc<MemoryReservation>,
}
impl<T> CapturedChildren<T> {
    pub(in crate::cells) fn new(capacity: usize, memory: &Arc<crate::MemoryBudget>) -> Result<Self> {
        vector_length(capacity)?;
        let bytes = capacity.checked_mul(std::mem::size_of::<T>()).and_then(|bytes| bytes.checked_add(128)).ok_or_else(allocation)?;
        let reservation = memory.reserve(bytes)?;
        Ok(Self { values: Vec::with_capacity(capacity), limit: capacity, _reservation: reservation })
    }
    pub(in crate::cells) fn push(&mut self, value: T) -> Result<()> {
        if self.values.len() == self.limit { return Err(allocation()); }
        self.values.push(value);
        Ok(())
    }
    pub(super) fn as_slice(&self) -> &[T] { &self.values }
    pub(super) fn capacity(&self) -> usize { self.values.capacity() }
}

pub(in crate::cells) struct NativeStructInput {
    key: String,
    pub(super) vector: ExecutionVector,
    _reservation: Arc<MemoryReservation>,
}
impl NativeStructInput {
    pub(in crate::cells) fn new(key: &str, vector: ExecutionVector, memory: &Arc<crate::MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(key.len().checked_add(128).ok_or_else(allocation)?)?;
        Ok(Self { key: key.to_owned(), vector, _reservation: reservation })
    }
    pub(super) fn key(&self) -> &str { &self.key }
}
