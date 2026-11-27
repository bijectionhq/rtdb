use std::sync::Arc;
use rtdb_columnar::MemoryBudget;
use rtdb_metrics::{OtlpValue, TraceRecorder, decode_trace_request};
use rtdb_plan::{TraceValue, Tracer};

#[tokio::test]
async fn bounded_queue_preserves_parentage_attributes_and_payload_lease_after_receiver_drop() {
    let memory = MemoryBudget::new(4 * 1024 * 1024);
    let (recorder, mut receiver) = TraceRecorder::new(Arc::clone(&memory), 2, true, false).unwrap();
    assert!(recorder.query_enabled()); assert!(!recorder.transaction_enabled());
    let mut parent = recorder.start(None, "xtdb.query", &[("query.text", TraceValue::Text("SELECT 1"))]).unwrap();
    let parent_context = parent.context();
    let child = recorder.start(Some(parent_context), "query.cursor.output", &[("cursor.page_count", TraceValue::Unsigned(1))]).unwrap();
    drop(child);
    parent.attribute("query.peak_memory_bytes", TraceValue::Unsigned(120));
    parent.attribute("signed", TraceValue::Integer(-4));
    parent.attribute("spill", TraceValue::Boolean(true));
    drop(parent);
    drop(recorder.start(None, "overflow", &[]).unwrap());
    assert_eq!(recorder.stats().dropped, 1);
    recorder.close();
    let child = receiver.next().await.unwrap(); let parent = receiver.next().await.unwrap();
    assert_eq!(child.parent, Some(parent.context));
    assert_eq!(child.context.trace_id, parent.context.trace_id);
    assert_ne!(child.context.span_id, parent.context.span_id);
    let child_payload = child.encode("native-test", &memory).unwrap().into_bytes();
    let child_request = decode_trace_request(&child_payload).unwrap();
    let encoded_child = &child_request.resource_spans[0].scope_spans[0].spans[0];
    assert_eq!(encoded_child.trace_id, child.context.trace_id.as_slice());
    assert_eq!(encoded_child.span_id, child.context.span_id.as_slice());
    assert_eq!(encoded_child.parent_span_id, parent.context.span_id.as_slice());
    drop(child_payload);
    let payload = parent.encode("native-test", &memory).unwrap().into_bytes();
    let request = decode_trace_request(&payload).unwrap();
    let resource_attrs = &request.resource_spans[0].resource.as_ref().unwrap().attributes;
    assert_eq!(resource_attrs[0].key, "service.name");
    assert_eq!(resource_attrs[0].value.as_ref().unwrap().value, Some(OtlpValue::StringValue("native-test".into())));
    assert_eq!(request.resource_spans[0].scope_spans[0].scope.as_ref().unwrap().name, "xtdb");
    let encoded = &request.resource_spans[0].scope_spans[0].spans[0];
    assert_eq!(encoded.name, "xtdb.query");
    assert_eq!(encoded.trace_id.len(), 16);
    assert_eq!(encoded.span_id.len(), 8);
    assert!(encoded.parent_span_id.is_empty());
    assert_eq!(encoded.kind, 1);
    assert_eq!(encoded.flags, 1);
    assert_eq!(encoded.attributes[0].value.as_ref().unwrap().value, Some(OtlpValue::StringValue("SELECT 1".into())));
    assert_eq!(encoded.attributes[1].value.as_ref().unwrap().value, Some(OtlpValue::IntValue(120)));
    assert_eq!(encoded.attributes[2].value.as_ref().unwrap().value, Some(OtlpValue::IntValue(-4)));
    assert_eq!(encoded.attributes[3].value.as_ref().unwrap().value, Some(OtlpValue::BoolValue(true)));
    assert!(receiver.next().await.is_none());
    drop(child); drop(parent); drop(receiver); drop(recorder);
    assert!(memory.used_bytes() > 0);
    drop(payload); assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn close_wakes_waiter_and_late_active_span_is_dropped_without_reopening_queue() {
    let memory = MemoryBudget::new(1_000_000);
    let (recorder, mut receiver) = TraceRecorder::new(Arc::clone(&memory), 1, true, true).unwrap();
    let late = recorder.start(None, "late", &[]).unwrap();
    let waiting = tokio::spawn(async move { assert!(receiver.next().await.is_none()); });
    recorder.close(); waiting.await.unwrap(); drop(late);
    assert_eq!(recorder.stats().dropped, 1);
    assert!(recorder.start(None, "closed", &[]).is_none());
    drop(recorder); assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn span_attribute_limit_counts_drops_and_replacement_keeps_last_value() {
    let memory = MemoryBudget::new(4 * 1024 * 1024);
    let (recorder, mut receiver) = TraceRecorder::new(Arc::clone(&memory), 1, true, true).unwrap();
    let mut span = recorder.start(None, "attributes", &[]).unwrap();
    for index in 0..130 { span.attribute(&format!("a{index}"), TraceValue::Unsigned(index)); }
    span.attribute("a0", TraceValue::Text("replacement")); drop(span); recorder.close();
    let record = receiver.next().await.unwrap();
    assert_eq!(record.attributes().count(), 128);
    assert_eq!(record.attributes().next(), Some(("a0", "replacement")));
    let payload = record.encode("test", &memory).unwrap();
    let request = decode_trace_request(payload.as_ref()).unwrap();
    assert_eq!(request.resource_spans[0].scope_spans[0].spans[0].dropped_attributes_count, 2);
}
