mod text;
mod recipe;

use std::{alloc::Layout, sync::{Arc, atomic::AtomicUsize}};
use rtdb_types::{AllocationScope, Anomaly, MemoryBudget, MemoryReservation};

type WriteResult<T> = std::result::Result<T, JsonWriteFailure>;

#[derive(Debug)]
enum JsonWriteFailure { Capacity, Admission(Anomaly), Internal }
impl From<Anomaly> for JsonWriteFailure {
    fn from(error: Anomaly) -> Self { Self::Admission(error) }
}

trait JsonSink { fn append(&mut self, bytes: &[u8]) -> WriteResult<()>; }

#[derive(Default)]
struct CountingSink { length: usize }
impl JsonSink for CountingSink {
    fn append(&mut self, bytes: &[u8]) -> WriteResult<()> {
        self.length = self.length.checked_add(bytes.len()).ok_or(JsonWriteFailure::Capacity)?;
        Ok(())
    }
}

struct BoundedSink<'scope> { bytes: Vec<u8>, limit: usize, allocation: &'scope AllocationScope }
impl<'scope> BoundedSink<'scope> {
    fn new(limit: usize, allocation: &'scope AllocationScope) -> WriteResult<Self> {
        let bytes = Layout::array::<u8>(limit).map_err(|_| JsonWriteFailure::Capacity)?.size();
        allocation.reserve(bytes)?;
        Ok(Self { bytes: Vec::with_capacity(limit), limit, allocation })
    }
    fn finish(self) -> WriteResult<JsonDocument> {
        if self.bytes.len() != self.limit { return Err(JsonWriteFailure::Internal); }
        let text = std::str::from_utf8(&self.bytes).map_err(|_| JsonWriteFailure::Internal)?;
        JsonDocument::copy(text, self.allocation.memory_budget())
    }
}
// The scratch scope remains borrowed until its buffer has been destroyed.
impl Drop for BoundedSink<'_> { fn drop(&mut self) {} }
impl JsonSink for BoundedSink<'_> {
    fn append(&mut self, bytes: &[u8]) -> WriteResult<()> {
        let length = self.bytes.len().checked_add(bytes.len()).ok_or(JsonWriteFailure::Capacity)?;
        if length > self.limit { return Err(JsonWriteFailure::Internal); }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct JsonDocument { storage: Arc<DocumentStorage>, reservation: Arc<MemoryReservation> }
#[derive(Debug)]
struct DocumentStorage { text: Box<str> }
impl JsonDocument {
    fn allocation_bound(length: usize) -> WriteResult<usize> {
        let bytes = Layout::array::<u8>(length).map_err(|_| JsonWriteFailure::Capacity)?.size();
        let owner = Layout::new::<[AtomicUsize; 2]>().extend(Layout::new::<DocumentStorage>())
            .map_err(|_| JsonWriteFailure::Capacity)?.0.pad_to_align();
        bytes.checked_add(owner.size()).and_then(|bytes| bytes.checked_add(256)).ok_or(JsonWriteFailure::Capacity)
    }
    fn copy(text: &str, memory: &Arc<MemoryBudget>) -> WriteResult<Self> {
        let reservation = memory.reserve(Self::allocation_bound(text.len())?)?;
        let storage = Arc::new(DocumentStorage { text: Box::from(text) });
        Ok(Self { storage, reservation })
    }
    fn as_str(&self) -> &str { &self.storage.text }
    fn retained_bytes(&self) -> usize { self.reservation.bytes() }
}

mod leaf {
    use std::io;
    use rtdb_types::Value;
    use super::{JsonSink, JsonWriteFailure, WriteResult};

    pub(super) struct LeafValue<'source>(&'source Value);
    impl<'source> LeafValue<'source> {
        pub(super) fn new(value: &'source Value) -> Option<Self> {
            match value {
                Value::List(_) | Value::Set(_) | Value::Map { .. } | Value::FixedSizeList(_) | Value::Struct(_) | Value::Keyword(_) => None,
                Value::Undefined | Value::Null | Value::Bool(_) | Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_)
                | Value::U8(_) | Value::U16(_) | Value::U32(_) | Value::U64(_) | Value::F16(_) | Value::F32(_) | Value::F64(_)
                | Value::Utf8(_) | Value::Binary(_) | Value::FixedSizeBinary(_) | Value::Decimal(_) | Value::Date(_) | Value::DateDay(_) | Value::DateMilli(_)
                | Value::Time { .. } | Value::Timestamp { .. } | Value::Duration { .. } | Value::IntervalYearMonth(_) | Value::IntervalDayTime { .. }
                | Value::IntervalMonthDayNano { .. } | Value::IntervalMonthDayMicro { .. } | Value::Uuid(_) | Value::Uri(_) | Value::Transit(_)
                | Value::Oid(_) | Value::RegClass(_) | Value::RegProc(_) | Value::TimestampTzRange { .. } => Some(Self(value)),
            }
        }
    }

    struct SerializerSink<'a, S> { sink: &'a mut S, failure: Option<JsonWriteFailure> }
    impl<S: JsonSink> io::Write for SerializerSink<'_, S> {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.failure.is_some() { return Err(io::ErrorKind::Other.into()); }
            match self.sink.append(bytes) {
                Ok(()) => Ok(bytes.len()),
                Err(error) => { self.failure = Some(error); Err(io::ErrorKind::Other.into()) }
            }
        }
        fn flush(&mut self) -> io::Result<()> { Ok(()) }
    }

    pub(super) fn serialize(value: LeafValue<'_>, sink: &mut impl JsonSink) -> WriteResult<()> {
        let mut adapter = SerializerSink { sink, failure: None };
        let result = serde::Serialize::serialize(value.0, &mut serde_json::Serializer::new(&mut adapter));
        if let Some(error) = adapter.failure { return Err(error); }
        result.map_err(|_| JsonWriteFailure::Internal)
    }
}
