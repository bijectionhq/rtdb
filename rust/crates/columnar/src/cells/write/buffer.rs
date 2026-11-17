use super::*;
use arrow_buffer::Buffer;

struct Allocation { bytes: Box<[u8]>, reservation: Arc<MemoryReservation> }
pub(super) struct MutableBytes {
    allocation: Option<Allocation>,
    pub append: usize,
    memory: Arc<MemoryBudget>,
}
impl MutableBytes {
    pub fn new(memory: &Arc<MemoryBudget>) -> Self { Self { allocation: None, append: 0, memory: Arc::clone(memory) } }
    pub fn bytes(&self) -> &[u8] { self.allocation.as_ref().map_or(&[], |allocation| allocation.bytes.as_ref()) }
    pub fn ensure_capacity(&mut self, requested: usize, mutation: &mut Mutation) -> WriteResult<()> {
        if requested <= self.bytes().len() { return Ok(()); }
        let mut extent = self.bytes().len().max(128);
        while extent < requested { extent = extent.checked_mul(2).ok_or_else(native_size)?; }
        let reservation = self.memory.reserve(extent.checked_add(128).ok_or_else(native_size)?)?;
        let mut bytes = vec![0; extent].into_boxed_slice();
        bytes[..self.bytes().len()].copy_from_slice(self.bytes());
        self.allocation = Some(Allocation { bytes, reservation });
        mutation.mark();
        Ok(())
    }
    pub fn ensure_writable(&mut self, length: usize, mutation: &mut Mutation) -> WriteResult<()> {
        self.ensure_capacity(self.append.checked_add(length).ok_or_else(native_size)?, mutation)
    }
    pub fn ensure_writable_i32(&mut self, length: i32, mutation: &mut Mutation) -> WriteResult<()> {
        if length > 0 { self.ensure_writable(length as usize, mutation)?; }
        Ok(())
    }
    pub fn set(&mut self, offset: usize, value: &[u8], mutation: &mut Mutation) -> WriteResult<()> {
        let end = offset.checked_add(value.len()).ok_or_else(native_size)?;
        if value.is_empty() && offset <= self.bytes().len() { return Ok(()); }
        let target = self.allocation.as_mut().and_then(|allocation| allocation.bytes.get_mut(offset..end)).ok_or_else(bounds)?;
        target.copy_from_slice(value);
        mutation.mark();
        Ok(())
    }
    pub fn fill(&mut self, offset: usize, length: usize, value: u8, mutation: &mut Mutation) -> WriteResult<()> {
        let end = offset.checked_add(length).ok_or_else(native_size)?;
        if length == 0 && offset <= self.bytes().len() { return Ok(()); }
        let target = self.allocation.as_mut().and_then(|allocation| allocation.bytes.get_mut(offset..end)).ok_or_else(bounds)?;
        target.fill(value);
        mutation.mark();
        Ok(())
    }
    pub fn append(&mut self, value: &[u8], mutation: &mut Mutation) -> WriteResult<()> {
        self.ensure_writable(value.len(), mutation)?;
        self.append_reserved(value, mutation)
    }
    pub fn append_reserved(&mut self, value: &[u8], mutation: &mut Mutation) -> WriteResult<()> {
        self.set(self.append, value, mutation)?;
        self.append = self.append.checked_add(value.len()).ok_or_else(native_size)?;
        if !value.is_empty() { mutation.mark(); }
        Ok(())
    }
    pub fn zero(&mut self, length: usize, mutation: &mut Mutation) -> WriteResult<()> {
        self.ensure_writable(length, mutation)?;
        let end = self.append.checked_add(length).ok_or_else(native_size)?;
        if let Some(allocation) = &mut self.allocation { allocation.bytes[self.append..end].fill(0); }
        self.append = end;
        if length != 0 { mutation.mark(); }
        Ok(())
    }
    pub fn clear(&mut self) { if let Some(allocation) = &mut self.allocation { allocation.bytes.fill(0); } self.append = 0; }
    pub fn finish(self) -> Result<StoredBytes> {
        let (buffer, reservation) = match self.allocation {
            Some(allocation) => (Buffer::from(allocation.bytes.into_vec()), allocation.reservation),
            None => (Buffer::from(Vec::<u8>::new()), self.memory.reserve(0)?),
        };
        Ok(StoredBytes { bytes: RetainedBuffer::with_reservation(buffer, reservation)?, append: self.append })
    }
}

pub(super) struct MutableBits { bytes: MutableBytes, pub append: i32 }
impl MutableBits {
    pub fn new(memory: &Arc<MemoryBudget>) -> Self { Self { bytes: MutableBytes::new(memory), append: 0 } }
    pub fn bytes(&self) -> &[u8] { self.bytes.bytes() }
    pub fn ensure_capacity_signed(&mut self, count: i32, mutation: &mut Mutation) -> WriteResult<()> {
        let bytes = (count.wrapping_add(7) as u32) >> 3;
        self.bytes.ensure_capacity(bytes as usize, mutation)
    }
    pub fn ensure_capacity(&mut self, count: usize, mutation: &mut Mutation) -> WriteResult<()> {
        self.ensure_capacity_signed(i32::try_from(count).map_err(|_| native_size())?, mutation)
    }
    pub fn ensure_writable(&mut self, count: usize, mutation: &mut Mutation) -> WriteResult<()> {
        self.ensure_capacity_signed(self.append.wrapping_add(i32::try_from(count).map_err(|_| native_size())?), mutation)
    }
    fn signed_byte(index: i32) -> WriteResult<usize> { usize::try_from(index).map_err(|_| bounds()) }
    fn set_signed(&mut self, row: i32, value: bool, mutation: &mut Mutation) -> WriteResult<()> {
        let byte = Self::signed_byte(row >> 3)?;
        let current = *self.bytes.bytes().get(byte).ok_or_else(bounds)?;
        let mask = 1 << (row & 7);
        self.bytes.set(byte, &[if value { current | mask } else { current & !mask }], mutation)
    }
    pub fn set(&mut self, row: usize, value: bool, mutation: &mut Mutation) -> WriteResult<()> {
        self.set_signed(i32::try_from(row).map_err(|_| bounds())?, value, mutation)
    }
    pub fn write_at(&mut self, row: usize, value: bool, mutation: &mut Mutation) -> WriteResult<()> {
        self.ensure_writable(1, mutation)?;
        self.set(row, value, mutation)?;
        self.append = (row as i32).wrapping_add(1);
        mutation.mark();
        Ok(())
    }
    pub fn write(&mut self, value: bool, mutation: &mut Mutation) -> WriteResult<()> {
        self.ensure_writable(1, mutation)?;
        let row = self.append;
        self.append = self.append.wrapping_add(1);
        mutation.mark();
        self.set_signed(row, value, mutation)
    }
    pub fn ones(&mut self, count: usize, mutation: &mut Mutation) -> WriteResult<()> {
        self.ensure_writable(count, mutation)?;
        let mut index = self.append;
        let mut remaining = i32::try_from(count).map_err(|_| native_size())?;
        if index % 8 != 0 {
            let first = (8 - index % 8).min(remaining);
            let byte = Self::signed_byte(index / 8)?;
            let mut tail = *self.bytes.bytes().get(byte).ok_or_else(bounds)?;
            for bit in 0..first { tail |= (1u32 << (index.wrapping_add(bit) % 8 & 31)) as u8; }
            self.bytes.set(byte, &[tail], mutation)?;
            index = index.wrapping_add(first);
            remaining -= first;
        }
        let full_bytes = remaining / 8;
        if full_bytes > 0 { self.bytes.fill(Self::signed_byte(index / 8)?, full_bytes as usize, 255, mutation)?; }
        index = index.wrapping_add(full_bytes * 8);
        remaining -= full_bytes * 8;
        if remaining != 0 {
            let tail = ((1u16 << remaining) - 1) as u8;
            self.bytes.set(Self::signed_byte(index / 8)?, &[tail], mutation)?;
            index = index.wrapping_add(remaining);
        }
        self.append = index;
        if count != 0 { mutation.mark(); }
        Ok(())
    }
    pub fn selected(&mut self, count: usize, mut source: impl FnMut(usize) -> WriteResult<bool>, mutation: &mut Mutation) -> WriteResult<()> {
        self.ensure_writable(count, mutation)?;
        let mut index = self.append;
        let byte = Self::signed_byte(index / 8)?;
        if byte > self.bytes.bytes().len() { return Err(bounds()); }
        self.bytes.append = byte;
        mutation.mark();
        let mut tail = if index % 8 == 0 { 0 } else { *self.bytes.bytes().get(byte).ok_or_else(bounds)? };
        for row in 0..count {
            let mask = (1u32 << (index % 8 & 31)) as u8;
            tail = if source(row)? { tail | mask } else { tail & !mask };
            index = index.wrapping_add(1);
            if index % 8 == 0 { self.bytes.append_reserved(&[tail], mutation)?; tail = 0; }
        }
        self.append = index;
        if index % 8 != 0 { self.bytes.append_reserved(&[tail], mutation)?; }
        Ok(())
    }
    pub fn clear(&mut self) { self.bytes.clear(); self.append = 0; }
    pub fn finish(self) -> Result<StoredBits> { let bytes = self.bytes.finish()?; Ok(StoredBits { bytes: bytes.bytes, append: self.append, byte_append: bytes.append }) }
}
