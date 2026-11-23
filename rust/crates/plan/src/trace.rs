use std::time::SystemTime;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpanContext { pub trace_id: [u8; 16], pub span_id: [u8; 8] }

#[derive(Clone, Copy, Debug)]
pub enum TraceValue<'a> { Text(&'a str), Integer(i64), Unsigned(u64), Boolean(bool) }

/// An owned span ends once when dropped; attributes are copied before each call returns.
/// `end_at` sets the recorded end instant and does not release the span's owned data.
pub trait TraceSpan: Send {
    fn context(&self) -> SpanContext;
    fn attribute(&mut self, name: &str, value: TraceValue<'_>);
    fn end_at(&mut self, end: SystemTime);
}

/// Starts spans synchronously without waiting for export; allocation admission may decline a span.
/// Completed spans may be dropped when the export queue is full.
/// Implementations must not panic or retain borrowed attribute data.
pub trait Tracer: Send + Sync {
    fn query_enabled(&self) -> bool;
    fn transaction_enabled(&self) -> bool;
    fn start(&self, parent: Option<SpanContext>, name: &str, attributes: &[(&str, TraceValue<'_>)]) -> Option<Box<dyn TraceSpan>>;
}
