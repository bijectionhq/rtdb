use super::*;
use arrow_buffer::Buffer;
use std::ops::Range;

pub(super) fn arrow19_default_capacity(request: u64) -> Result<usize> {
    let capacity = if request == 0 { 0 } else if request < 16 * 1024 * 1024 { request.next_power_of_two().max(2) } else { request };
    let capacity = i64::try_from(capacity).map_err(|_| allocation())?;
    let capacity = usize::try_from(capacity).map_err(|_| allocation())?;
    std::alloc::Layout::array::<u8>(capacity).map_err(|_| allocation())?;
    Ok(capacity)
}

struct Allocation { bytes: Box<[u8]>, reservation: Arc<MemoryReservation> }
pub(super) struct MutableBytes {
    allocation: Option<Allocation>,
    pub append: i64,
    memory: Arc<MemoryBudget>,
}
impl MutableBytes {
    pub fn new(memory: &Arc<MemoryBudget>) -> Self { Self { allocation: None, append: 0, memory: Arc::clone(memory) } }
    pub fn initial_arrow19_default_zeroed(request: u64, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let extent = arrow19_default_capacity(request)?;
        if extent == 0 { return Ok(Self::new(memory)); }
        let reservation = memory.reserve(extent.checked_add(128).ok_or_else(allocation)?)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(extent).map_err(|_| allocation())?;
        bytes.resize(extent, 0);
        Ok(Self { allocation: Some(Allocation { bytes: bytes.into_boxed_slice(), reservation }), append: 0, memory: Arc::clone(memory) })
    }
    pub fn bytes(&self) -> &[u8] { self.allocation.as_ref().map_or(&[], |allocation| allocation.bytes.as_ref()) }
    fn capacity(&self) -> i64 { self.bytes().len() as i64 }
    fn reallocate(&mut self, requested: i64, mutation: &mut Mutation) -> WriteResult<()> {
        let mut extent = self.bytes().len().max(128);
        while i64::try_from(extent).map_err(|_| native_size())? < requested { extent = extent.checked_mul(2).ok_or_else(native_size)?; }
        std::alloc::Layout::array::<u8>(extent).map_err(|_| native_size())?;
        let reservation = self.memory.reserve(extent.checked_add(128).ok_or_else(native_size)?)?;
        let mut bytes = vec![0; extent].into_boxed_slice();
        bytes[..self.bytes().len()].copy_from_slice(self.bytes());
        self.allocation = Some(Allocation { bytes, reservation });
        mutation.mark();
        Ok(())
    }
    pub fn ensure_capacity(&mut self, requested: usize, mutation: &mut Mutation) -> WriteResult<()> {
        self.ensure_capacity_signed(i64::try_from(requested).map_err(|_| native_size())?, mutation)
    }
    pub fn ensure_capacity_signed(&mut self, requested: i64, mutation: &mut Mutation) -> WriteResult<()> {
        if self.capacity() < requested { self.reallocate(requested, mutation)?; }
        Ok(())
    }
    pub fn ensure_writable(&mut self, length: i64, mutation: &mut Mutation) -> WriteResult<()> {
        if self.capacity().wrapping_sub(self.append) < length { self.reallocate(self.append.wrapping_add(length), mutation)?; }
        Ok(())
    }
    pub fn ensure_writable_i32(&mut self, length: i32, mutation: &mut Mutation) -> WriteResult<()> {
        self.ensure_writable(i64::from(length), mutation)
    }
    fn range(&self, offset: i64, length: i64) -> WriteResult<Range<usize>> {
        let start = usize::try_from(offset).map_err(|_| bounds())?;
        let length = usize::try_from(length).map_err(|_| bounds())?;
        let end = start.checked_add(length).filter(|end| *end <= self.bytes().len()).ok_or_else(bounds)?;
        Ok(start..end)
    }
    pub fn byte(&self, offset: i64) -> WriteResult<u8> { Ok(self.bytes()[self.range(offset, 1)?.start]) }
    fn put(&mut self, range: Range<usize>, value: &[u8], mutation: &mut Mutation) {
        if value.is_empty() { return; }
        self.allocation.as_mut().expect("admitted byte span").bytes[range].copy_from_slice(value);
        mutation.mark();
    }
    fn advance(&mut self, length: i64, mutation: &mut Mutation) {
        self.append = self.append.wrapping_add(length);
        if length != 0 { mutation.mark(); }
    }
    fn writable(&self, length: i64) -> WriteResult<()> {
        if length > self.capacity().wrapping_sub(self.append) { Err(bounds()) } else { Ok(()) }
    }
    pub fn set(&mut self, offset: i64, value: &[u8], mutation: &mut Mutation) -> WriteResult<()> {
        let range = self.range(offset, i64::try_from(value.len()).map_err(|_| native_size())?)?;
        self.put(range, value, mutation);
        Ok(())
    }
    pub fn fill(&mut self, offset: i64, length: i64, value: u8, mutation: &mut Mutation) -> WriteResult<()> {
        if length == 0 { return Ok(()); }
        let range = self.range(offset, length)?;
        self.allocation.as_mut().expect("admitted byte span").bytes[range].fill(value);
        mutation.mark();
        Ok(())
    }
    pub fn append_primitive(&mut self, value: &[u8], mutation: &mut Mutation) -> WriteResult<()> {
        self.ensure_writable(i64::try_from(value.len()).map_err(|_| native_size())?, mutation)?;
        self.append_primitive_reserved(value, mutation)
    }
    pub fn append_primitive_reserved(&mut self, value: &[u8], mutation: &mut Mutation) -> WriteResult<()> {
        let length = i64::try_from(value.len()).map_err(|_| native_size())?;
        self.writable(length)?;
        let range = self.range(self.append, length).map_err(|_| unsafe_cursor())?;
        self.put(range, value, mutation);
        self.advance(length, mutation);
        Ok(())
    }
    pub fn append_array(&mut self, value: &[u8], mutation: &mut Mutation) -> WriteResult<()> {
        let length = i64::try_from(value.len()).map_err(|_| native_size())?;
        self.ensure_writable(length, mutation)?;
        self.writable(length)?;
        let range = self.range(self.append, length)?;
        self.put(range, value, mutation);
        self.advance(length, mutation);
        Ok(())
    }
    pub fn copy_from_reserved<'a>(&mut self, length: i64, source: impl FnOnce() -> WriteResult<&'a [u8]>, mutation: &mut Mutation) -> WriteResult<()> {
        let range = self.range(self.append, length)?;
        let source = source()?;
        self.put(range, source, mutation);
        self.advance(length, mutation);
        Ok(())
    }
    pub fn zero(&mut self, length: usize, mutation: &mut Mutation) -> WriteResult<()> {
        let length = i64::try_from(length).map_err(|_| native_size())?;
        self.zero_extent(length, mutation)
    }
    pub fn zero_signed(&mut self, length: i32, mutation: &mut Mutation) -> WriteResult<()> {
        self.zero_extent(i64::from(length), mutation)
    }
    fn zero_extent(&mut self, length: i64, mutation: &mut Mutation) -> WriteResult<()> {
        self.ensure_writable(length, mutation)?;
        self.fill(self.append, length, 0, mutation)?;
        self.advance(length, mutation);
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
    pub fn initial_arrow19_default_zeroed(initial_count: u32, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let request = u64::from(initial_count.wrapping_add(7) >> 3);
        Ok(Self { bytes: MutableBytes::initial_arrow19_default_zeroed(request, memory)?, append: 0 })
    }
    pub fn bytes(&self) -> &[u8] { self.bytes.bytes() }
    pub fn byte_append(&self) -> i64 { self.bytes.append }
    pub fn ensure_capacity_signed(&mut self, count: i32, mutation: &mut Mutation) -> WriteResult<()> {
        let bytes = (count.wrapping_add(7) as u32) >> 3;
        self.bytes.ensure_capacity(bytes as usize, mutation)
    }
    pub fn ensure_capacity(&mut self, count: usize, mutation: &mut Mutation) -> WriteResult<()> {
        self.ensure_capacity_signed(i32::try_from(count).map_err(|_| native_size())?, mutation)
    }
    pub fn ensure_writable(&mut self, count: i32, mutation: &mut Mutation) -> WriteResult<()> {
        self.ensure_capacity_signed(self.append.wrapping_add(count), mutation)
    }
    pub fn set_signed(&mut self, row: i32, value: bool, mutation: &mut Mutation) -> WriteResult<()> {
        let byte = i64::from(row >> 3);
        let current = self.bytes.byte(byte)?;
        let mask = 1 << (row & 7);
        self.bytes.set(byte, &[if value { current | mask } else { current & !mask }], mutation)
    }
    pub fn set(&mut self, row: usize, value: bool, mutation: &mut Mutation) -> WriteResult<()> {
        self.set_signed(i32::try_from(row).map_err(|_| bounds())?, value, mutation)
    }
    pub fn write_at(&mut self, row: i32, value: bool, mutation: &mut Mutation) -> WriteResult<()> {
        self.ensure_writable(1, mutation)?;
        self.set_signed(row, value, mutation)?;
        self.append = row.wrapping_add(1);
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
    pub fn ones(&mut self, count: i32, mutation: &mut Mutation) -> WriteResult<()> {
        self.ensure_writable(count, mutation)?;
        let mut index = self.append;
        let mut remaining = count;
        if index % 8 != 0 {
            let first = (8 - index % 8).min(remaining);
            let byte = i64::from(index / 8);
            let mut tail = self.bytes.byte(byte)?;
            for bit in 0..first { tail |= (1u32 << (index.wrapping_add(bit) % 8 & 31)) as u8; }
            self.bytes.set(byte, &[tail], mutation)?;
            index = index.wrapping_add(first);
            remaining = remaining.wrapping_sub(first);
        }
        let full_bytes = remaining / 8;
        if full_bytes > 0 {
            self.bytes.fill(i64::from(index / 8), i64::from(full_bytes), 255, mutation)?;
            index = index.wrapping_add(full_bytes.wrapping_mul(8));
            remaining = remaining.wrapping_sub(full_bytes.wrapping_mul(8));
        }
        if remaining > 0 {
            let tail = ((1u16 << remaining) - 1) as u8;
            self.bytes.set(i64::from(index / 8), &[tail], mutation)?;
            index = index.wrapping_add(remaining);
        }
        self.append = index;
        if count != 0 { mutation.mark(); }
        Ok(())
    }
    pub fn selected(&mut self, count: i32, mut source: impl FnMut(i32) -> WriteResult<bool>, mutation: &mut Mutation) -> WriteResult<()> {
        self.ensure_writable(count, mutation)?;
        let mut index = self.append;
        self.bytes.append = i64::from(index / 8);
        mutation.mark();
        let mut tail = if index % 8 == 0 { 0 } else { self.bytes.byte(self.bytes.append)? };
        for row in 0..count {
            let mask = (1u32 << (index % 8 & 31)) as u8;
            tail = if source(row)? { tail | mask } else { tail & !mask };
            index = index.wrapping_add(1);
            if index % 8 == 0 { self.bytes.append_primitive_reserved(&[tail], mutation)?; tail = 0; }
        }
        self.append = index;
        if index % 8 != 0 { self.bytes.append_primitive_reserved(&[tail], mutation)?; }
        Ok(())
    }
    pub fn clear(&mut self) { self.bytes.clear(); self.append = 0; }
    pub fn finish(self) -> Result<StoredBits> { let bytes = self.bytes.finish()?; Ok(StoredBits { bytes: bytes.bytes, append: self.append, byte_append: bytes.append }) }
}
