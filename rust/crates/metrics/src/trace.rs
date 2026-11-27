use std::collections::VecDeque;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::time::{SystemTime, UNIX_EPOCH};
use parking_lot::Mutex;
use prost::Message;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::{SpanContext, TraceSpan, TraceValue, Tracer};
use rtdb_types::{Anomaly, Result};
use tokio::sync::Notify;

/// Content-Type for OTLP/HTTP protobuf trace requests and responses, matching upstream `OtlpHttpSpanExporter`.
pub const OTLP_PROTOBUF_CONTENT_TYPE: &str = "application/x-protobuf";

/// `opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct OtlpExportTraceRequest {
    #[prost(message, repeated, tag = "1")]
    pub resource_spans: Vec<OtlpResourceSpans>,
}

/// `opentelemetry.proto.trace.v1.ResourceSpans`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct OtlpResourceSpans {
    #[prost(message, optional, tag = "1")]
    pub resource: Option<OtlpResource>,
    #[prost(message, repeated, tag = "2")]
    pub scope_spans: Vec<OtlpScopeSpans>,
}

/// `opentelemetry.proto.resource.v1.Resource`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct OtlpResource {
    #[prost(message, repeated, tag = "1")]
    pub attributes: Vec<OtlpKeyValue>,
}

/// `opentelemetry.proto.trace.v1.ScopeSpans`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct OtlpScopeSpans {
    #[prost(message, optional, tag = "1")]
    pub scope: Option<OtlpInstrumentationScope>,
    #[prost(message, repeated, tag = "2")]
    pub spans: Vec<OtlpSpan>,
}

/// `opentelemetry.proto.common.v1.InstrumentationScope`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct OtlpInstrumentationScope {
    #[prost(string, tag = "1")]
    pub name: String,
}

/// `opentelemetry.proto.trace.v1.Span`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct OtlpSpan {
    #[prost(bytes = "vec", tag = "1")]
    pub trace_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub span_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    pub parent_span_id: Vec<u8>,
    #[prost(string, tag = "5")]
    pub name: String,
    #[prost(int32, tag = "6")]
    pub kind: i32,
    #[prost(fixed64, tag = "7")]
    pub start_time_unix_nano: u64,
    #[prost(fixed64, tag = "8")]
    pub end_time_unix_nano: u64,
    #[prost(message, repeated, tag = "9")]
    pub attributes: Vec<OtlpKeyValue>,
    #[prost(uint32, tag = "10")]
    pub dropped_attributes_count: u32,
    #[prost(fixed32, tag = "16")]
    pub flags: u32,
}

/// `opentelemetry.proto.common.v1.KeyValue`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct OtlpKeyValue {
    #[prost(string, tag = "1")]
    pub key: String,
    #[prost(message, optional, tag = "2")]
    pub value: Option<OtlpAnyValue>,
}

/// `opentelemetry.proto.common.v1.AnyValue`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct OtlpAnyValue {
    #[prost(oneof = "OtlpValue", tags = "1, 2, 3")]
    pub value: Option<OtlpValue>,
}

/// Supported scalar legs of `opentelemetry.proto.common.v1.AnyValue`.
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum OtlpValue {
    #[prost(string, tag = "1")]
    StringValue(String),
    #[prost(bool, tag = "2")]
    BoolValue(bool),
    #[prost(int64, tag = "3")]
    IntValue(i64),
}

/// `opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct OtlpExportTraceResponse {
    #[prost(message, optional, tag = "1")]
    pub partial_success: Option<OtlpExportTracePartialSuccess>,
}

/// `opentelemetry.proto.collector.trace.v1.ExportTracePartialSuccess`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct OtlpExportTracePartialSuccess {
    #[prost(int64, tag = "1")]
    pub rejected_spans: i64,
    #[prost(string, tag = "2")]
    pub error_message: String,
}

/// Decodes an OTLP/HTTP protobuf `ExportTraceServiceRequest`.
pub fn decode_trace_request(bytes: &[u8]) -> Result<OtlpExportTraceRequest> {
    OtlpExportTraceRequest::decode(bytes).map_err(|error| Anomaly::incorrect("rtdb.tracing/decode", error.to_string()))
}

/// Decodes an OTLP/HTTP protobuf `ExportTraceServiceResponse`.
pub fn decode_trace_response(bytes: &[u8]) -> Result<OtlpExportTraceResponse> {
    OtlpExportTraceResponse::decode(bytes).map_err(|error| Anomaly::incorrect("rtdb.tracing/decode", error.to_string()))
}

/// Encodes an OTLP/HTTP protobuf `ExportTraceServiceResponse`.
pub fn encode_trace_response(response: &OtlpExportTraceResponse) -> Vec<u8> {
    response.encode_to_vec()
}

fn string_attribute(key: &str, value: &str) -> OtlpKeyValue {
    OtlpKeyValue { key: key.into(), value: Some(OtlpAnyValue { value: Some(OtlpValue::StringValue(value.into())) }) }
}

const ATTRIBUTES: usize = 128;
struct Attribute { name: String, value: AttributeValue, _reservation: Arc<MemoryReservation> }

enum AttributeValue { Text(String), Integer(i64, String), Boolean(bool) }

impl AttributeValue {
    fn text(&self) -> &str {
        match self {
            Self::Text(value) | Self::Integer(_, value) => value,
            Self::Boolean(true) => "true",
            Self::Boolean(false) => "false",
        }
    }

    fn otlp(&self) -> OtlpAnyValue {
        let value = match self {
            Self::Text(value) => OtlpValue::StringValue(value.clone()),
            Self::Integer(value, _) => OtlpValue::IntValue(*value),
            Self::Boolean(value) => OtlpValue::BoolValue(*value),
        };
        OtlpAnyValue { value: Some(value) }
    }
}
pub struct TraceRecord {
    pub context: SpanContext,
    pub parent: Option<SpanContext>,
    pub name: String,
    pub started: SystemTime,
    pub ended: SystemTime,
    attributes: Vec<Attribute>,
    dropped_attributes: u32,
    _reservation: Arc<MemoryReservation>,
}
impl TraceRecord {
    pub fn attributes(&self) -> impl Iterator<Item = (&str, &str)> { self.attributes.iter().map(|attribute| (attribute.name.as_str(), attribute.value.text())) }
    /// Encodes one span as an OTLP/HTTP protobuf `ExportTraceServiceRequest`, retaining the complete payload allocation.
    pub fn encode(&self, service: &str, memory: &Arc<MemoryBudget>) -> Result<TracePayload> {
        let bytes = self.attributes.iter().try_fold(size(size(self.name.len(), service.len())?, 8192)?, |bytes, attribute| size(bytes, size(attribute.name.len(), attribute.value.text().len())?))?;
        let reservation = memory.reserve(bytes.checked_mul(16).ok_or_else(overflow)?)?;
        let request = OtlpExportTraceRequest {
            resource_spans: vec![OtlpResourceSpans {
                resource: Some(OtlpResource {
                    attributes: vec![
                        string_attribute("service.name", service),
                        string_attribute("telemetry.sdk.language", "rust"),
                        string_attribute("telemetry.sdk.name", "rtdb"),
                        string_attribute("telemetry.sdk.version", env!("CARGO_PKG_VERSION")),
                    ],
                }),
                scope_spans: vec![OtlpScopeSpans {
                    scope: Some(OtlpInstrumentationScope { name: "xtdb".into() }),
                    spans: vec![OtlpSpan {
                        trace_id: self.context.trace_id.to_vec(),
                        span_id: self.context.span_id.to_vec(),
                        parent_span_id: self.parent.map_or_else(Vec::new, |parent| parent.span_id.to_vec()),
                        name: self.name.clone(),
                        kind: 1,
                        start_time_unix_nano: nanos(self.started),
                        end_time_unix_nano: nanos(self.ended),
                        attributes: self.attributes.iter().map(|attribute| OtlpKeyValue {
                            key: attribute.name.clone(),
                            value: Some(attribute.value.otlp()),
                        }).collect(),
                        dropped_attributes_count: self.dropped_attributes,
                        flags: 1,
                    }],
                }],
            }],
        };
        let bytes = request.encode_to_vec();
        Ok(TracePayload { bytes, _reservation: reservation })
    }
}
pub struct TracePayload { bytes: Vec<u8>, _reservation: Arc<MemoryReservation> }
impl AsRef<[u8]> for TracePayload { fn as_ref(&self) -> &[u8] { &self.bytes } }
impl TracePayload { pub fn into_bytes(self) -> bytes::Bytes { bytes::Bytes::from_owner(self) } }

struct Queue { closed: bool, records: VecDeque<TraceRecord> }
struct Shared {
    queue: Mutex<Queue>, ready: Notify, capacity: usize, memory: Arc<MemoryBudget>,
    dropped: AtomicU64, exported: AtomicU64, failed: AtomicU64, _reservation: Arc<MemoryReservation>,
}
pub struct TraceRecorder { shared: Arc<Shared>, query: bool, transaction: bool }
pub struct TraceReceiver { shared: Arc<Shared> }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraceStats { pub dropped: u64, pub exported: u64, pub failed: u64 }
impl TraceRecorder {
    /// Capacity bounds completed spans; active spans and their attribute copies charge the shared budget.
    /// Keep the receiver with its exporter owner; closing it discards subsequent completed spans.
    pub fn new(memory: Arc<MemoryBudget>, capacity: usize, query: bool, transaction: bool) -> Result<(Arc<Self>, TraceReceiver)> {
        if capacity == 0 { return Err(Anomaly::incorrect("rtdb.tracing/capacity", "Trace capacity must be positive")); }
        let reservation = memory.reserve(capacity.checked_mul(std::mem::size_of::<TraceRecord>()).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?)?;
        let shared = Arc::new(Shared { queue: Mutex::new(Queue { closed: false, records: VecDeque::with_capacity(capacity) }), ready: Notify::new(), capacity, memory,
            dropped: AtomicU64::new(0), exported: AtomicU64::new(0), failed: AtomicU64::new(0), _reservation: reservation });
        Ok((Arc::new(Self { shared: Arc::clone(&shared), query, transaction }), TraceReceiver { shared }))
    }
    /// Closes admission to the queue; its receiver drains records already admitted.
    pub fn close(&self) { self.shared.queue.lock().closed = true; self.shared.ready.notify_waiters(); }
    pub fn stats(&self) -> TraceStats { TraceStats { dropped: self.shared.dropped.load(Ordering::Relaxed), exported: self.shared.exported.load(Ordering::Relaxed), failed: self.shared.failed.load(Ordering::Relaxed) } }
}
impl Tracer for TraceRecorder {
    fn query_enabled(&self) -> bool { self.query }
    fn transaction_enabled(&self) -> bool { self.transaction }
    fn start(&self, parent: Option<SpanContext>, name: &str, attributes: &[(&str, TraceValue<'_>)]) -> Option<Box<dyn TraceSpan>> {
        if self.shared.queue.lock().closed { return None; }
        let bytes = name.len().checked_add(4096 + ATTRIBUTES * std::mem::size_of::<Attribute>())?;
        let reservation = match self.shared.memory.reserve(bytes) { Ok(reservation) => reservation, Err(_) => { self.shared.dropped.fetch_add(1, Ordering::Relaxed); return None; } };
        let trace_id = parent.map_or_else(|| *uuid::Uuid::new_v4().as_bytes(), |parent| parent.trace_id);
        let mut span_id = [0; 8]; span_id.copy_from_slice(&uuid::Uuid::new_v4().as_bytes()[..8]);
        let now = SystemTime::now();
        let mut span = RecordedSpan { shared: Arc::clone(&self.shared), record: Some(TraceRecord { context: SpanContext { trace_id, span_id }, parent, name: name.into(), started: now, ended: now, attributes: Vec::with_capacity(ATTRIBUTES), dropped_attributes: 0, _reservation: reservation }), explicit_end: false };
        for (name, value) in attributes { span.attribute(name, *value); }
        Some(Box::new(span))
    }
}
struct RecordedSpan { shared: Arc<Shared>, record: Option<TraceRecord>, explicit_end: bool }
impl TraceSpan for RecordedSpan {
    fn context(&self) -> SpanContext { self.record.as_ref().expect("live span owns its record").context }
    fn attribute(&mut self, name: &str, value: TraceValue<'_>) {
        let record = self.record.as_mut().expect("live span owns its record");
        let existing = record.attributes.iter().position(|attribute| attribute.name == name);
        if existing.is_none() && record.attributes.len() == ATTRIBUTES { record.dropped_attributes = record.dropped_attributes.saturating_add(1); return; }
        let length = match value { TraceValue::Text(value) => value.len(), _ => 32 };
        let reservation = name.len().checked_add(length).and_then(|bytes| bytes.checked_add(256)).and_then(|bytes| self.shared.memory.reserve(bytes).ok());
        let Some(reservation) = reservation else { record.dropped_attributes = record.dropped_attributes.saturating_add(1); return; };
        let value = match value {
            TraceValue::Text(value) => AttributeValue::Text(value.to_owned()),
            TraceValue::Integer(value) => AttributeValue::Integer(value, value.to_string()),
            TraceValue::Unsigned(value) => {
                let clamped = value.min(i64::MAX as u64) as i64;
                AttributeValue::Integer(clamped, clamped.to_string())
            }
            TraceValue::Boolean(value) => AttributeValue::Boolean(value),
        };
        let attribute = Attribute { name: name.into(), value, _reservation: reservation };
        if let Some(index) = existing { record.attributes[index] = attribute; } else { record.attributes.push(attribute); }
    }
    fn end_at(&mut self, end: SystemTime) { self.record.as_mut().expect("live span owns its record").ended = end; self.explicit_end = true; }
}
impl Drop for RecordedSpan {
    fn drop(&mut self) {
        let Some(mut record) = self.record.take() else { return; };
        if !self.explicit_end { record.ended = SystemTime::now(); }
        let mut queue = self.shared.queue.lock();
        if queue.closed || queue.records.len() == self.shared.capacity { self.shared.dropped.fetch_add(1, Ordering::Relaxed); return; }
        queue.records.push_back(record); drop(queue); self.shared.ready.notify_one();
    }
}
impl TraceReceiver {
    /// One exporter owns the receiver; cancellation leaves the next queued record available to it.
    pub async fn next(&mut self) -> Option<TraceRecord> {
        loop {
            let ready = self.shared.ready.notified();
            tokio::pin!(ready); ready.as_mut().enable();
            { let mut queue = self.shared.queue.lock(); if let Some(record) = queue.records.pop_front() { return Some(record); } if queue.closed { return None; } }
            ready.await;
        }
    }
    pub fn exported(&self) { self.shared.exported.fetch_add(1, Ordering::Relaxed); }
    pub fn failed(&self) { self.shared.failed.fetch_add(1, Ordering::Relaxed); }
}
impl Drop for TraceReceiver {
    fn drop(&mut self) { let mut queue = self.shared.queue.lock(); queue.closed = true; self.shared.dropped.fetch_add(queue.records.len() as u64, Ordering::Relaxed); queue.records.clear(); drop(queue); self.shared.ready.notify_waiters(); }
}
fn nanos(time: SystemTime) -> u64 { time.duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos().min(u64::MAX as u128) as u64 }
fn size(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(overflow) }
fn overflow() -> Anomaly { Anomaly::incorrect("rtdb.tracing/size", "Trace allocation size overflow") }
