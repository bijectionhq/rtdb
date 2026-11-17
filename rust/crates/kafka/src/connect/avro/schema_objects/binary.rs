use super::*;

pub(in crate::connect::avro) struct ByteStorage { bytes: Vec<u8>, _charge: Arc<MemoryReservation> }
#[derive(Clone, Copy, Debug)]
pub(in crate::connect::avro) enum BufferBacking { Heap { offset: usize, read_only: bool }, Direct { read_only: bool } }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::connect::avro) enum BufferOrder { BigEndian, LittleEndian }
#[derive(Clone, Debug)]
pub(in crate::connect::avro) struct ByteBuffer { storage: Arc<ByteStorage>, backing: BufferBacking, capacity: usize, position: usize, limit: usize, mark: Option<usize>, order: BufferOrder }

impl std::fmt::Debug for ByteStorage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.debug_struct("ByteStorage").field("length", &self.bytes.len()).finish() }
}
impl ByteStorage {
    pub(super) fn new(bytes: Vec<u8>, charge: Arc<MemoryReservation>) -> Self { Self { bytes, _charge: charge } }
    pub(in crate::connect::avro) fn bytes(&self) -> &[u8] { &self.bytes }
    pub(super) fn retain(&self, context: &mut ConversionContext) -> Result<()> { context.retain(Arc::clone(&self._charge)) }
}
impl ByteBuffer {
    pub(super) fn class_name(&self) -> &'static str {
        match self.backing {
            BufferBacking::Heap { read_only: false, .. } => "java.nio.HeapByteBuffer", BufferBacking::Heap { read_only: true, .. } => "java.nio.HeapByteBufferR",
            BufferBacking::Direct { read_only: false } => "java.nio.DirectByteBuffer", BufferBacking::Direct { read_only: true } => "java.nio.DirectByteBufferR",
        }
    }
    pub(in crate::connect::avro) fn whole(storage: Arc<ByteStorage>) -> Self {
        let capacity = storage.bytes.len(); Self { storage, backing: BufferBacking::Heap { offset: 0, read_only: false }, capacity, position: 0, limit: capacity, mark: None, order: BufferOrder::BigEndian }
    }
    pub(in crate::connect::avro) fn layout(storage: Arc<ByteStorage>, backing: BufferBacking, capacity: usize, position: usize, limit: usize) -> Result<Self> {
        Self::state(storage, backing, capacity, position, limit, None, BufferOrder::BigEndian)
    }
    pub(in crate::connect::avro) fn state(storage: Arc<ByteStorage>, backing: BufferBacking, capacity: usize, position: usize, limit: usize, mark: Option<usize>, order: BufferOrder) -> Result<Self> {
        let offset = match backing { BufferBacking::Heap { offset, .. } => offset, BufferBacking::Direct { .. } => 0 };
        if capacity > i32::MAX as usize || position > limit || limit > capacity || mark.is_some_and(|mark| mark > position) || add(offset, capacity)? > storage.bytes.len() { return Err(invalid("ByteBuffer layout exceeds its admitted backing storage")); }
        Ok(Self { storage, backing, capacity, position, limit, mark, order })
    }
    pub(in crate::connect::avro) fn remaining(&self) -> &[u8] {
        let offset = match self.backing { BufferBacking::Heap { offset, .. } => offset, BufferBacking::Direct { .. } => 0 };
        &self.storage.bytes[offset + self.position..offset + self.limit]
    }
    pub(in crate::connect::avro) fn backing_array(&self) -> Result<&Arc<ByteStorage>> {
        match self.backing {
            BufferBacking::Heap { read_only: false, .. } => Ok(&self.storage),
            BufferBacking::Heap { read_only: true, .. } => Err(error("avro-value-unchecked", "A read-only ByteBuffer does not expose its backing array")),
            BufferBacking::Direct { .. } => Err(error("avro-value-unchecked", "A direct ByteBuffer does not expose a backing array")),
        }
    }
    pub(in crate::connect::avro) fn capacity(&self) -> usize { self.capacity }
    pub(in crate::connect::avro) fn position(&self) -> usize { self.position }
    pub(in crate::connect::avro) fn limit(&self) -> usize { self.limit }
    pub(in crate::connect::avro) fn mark(&self) -> Option<usize> { self.mark }
    pub(in crate::connect::avro) fn order(&self) -> BufferOrder { self.order }
    pub(in crate::connect::avro) fn read_only(&self) -> bool { match self.backing { BufferBacking::Heap { read_only, .. } | BufferBacking::Direct { read_only } => read_only } }
    pub(super) fn retain(&self, context: &mut ConversionContext) -> Result<()> { self.storage.retain(context) }
}
