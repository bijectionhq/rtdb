use std::sync::Arc;
use rtdb_columnar::MemoryBudget;
use rtdb_plan::{TraceValue, Tracer};
use rtdb_server::{config::TracingSettings, tracing::TraceExporter};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn capture(response_body: &'static str) -> (std::net::SocketAddr, tokio::task::JoinHandle<serde_json::Value>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new(); let mut chunk = [0; 4096];
        let header_end = loop {
            let count = stream.read(&mut chunk).await.unwrap(); assert_ne!(count, 0); bytes.extend_from_slice(&chunk[..count]);
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") { break index + 4; }
        };
        let headers = std::str::from_utf8(&bytes[..header_end]).unwrap();
        assert!(headers.starts_with("POST /v1/traces HTTP/1.1\r\n"));
        assert!(headers.to_ascii_lowercase().contains("content-type: application/json"));
        let length = headers.lines().find_map(|line| { let (name, value) = line.split_once(':')?; name.eq_ignore_ascii_case("content-length").then(|| value.trim().parse::<usize>().unwrap()) }).unwrap();
        while bytes.len() < header_end + length { let count = stream.read(&mut chunk).await.unwrap(); assert_ne!(count, 0); bytes.extend_from_slice(&chunk[..count]); }
        let value = serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap();
        let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}", response_body.len());
        stream.write_all(response.as_bytes()).await.unwrap(); value
    });
    (address, task)
}

#[tokio::test]
async fn configured_otlp_endpoint_receives_charged_spans_before_shutdown_returns() {
    let (address, server) = capture("{}").await;
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let settings = TracingSettings { enabled: true, endpoint: format!("http://{address}/v1/traces"), service_name: "configured-native".into(), query_tracing: true, transaction_tracing: false };
    let (handle, exporter) = TraceExporter::open(&settings, Arc::clone(&memory)).unwrap();
    drop(handle.recorder().start(None, "xtdb.query", &[("query.text", TraceValue::Text("SELECT 42"))]).unwrap());
    handle.close(); exporter.run().await;
    let request = server.await.unwrap();
    assert_eq!(request["resourceSpans"][0]["resource"]["attributes"][0]["value"]["stringValue"], "configured-native");
    assert_eq!(request["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["name"], "xtdb.query");
    assert_eq!(handle.recorder().stats().exported, 1); assert_eq!(handle.recorder().stats().failed, 0);
    drop(handle); assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn otlp_partial_rejection_is_counted_without_retrying_accepted_http_request() {
    let (address, server) = capture("{\"partialSuccess\":{\"rejectedSpans\":\"1\",\"errorMessage\":\"invalid span\"}}").await;
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let settings = TracingSettings { enabled: true, endpoint: format!("http://{address}/v1/traces"), service_name: "test".into(), query_tracing: true, transaction_tracing: true };
    let (handle, exporter) = TraceExporter::open(&settings, memory).unwrap();
    drop(handle.recorder().start(None, "rejected", &[]).unwrap()); handle.close(); exporter.run().await; server.await.unwrap();
    assert_eq!(handle.recorder().stats().failed, 1); assert_eq!(handle.recorder().stats().exported, 0);
}
