use std::{collections::{BTreeMap, BTreeSet}, sync::Arc, time::Duration};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::CatalogMetric;
use rtdb_types::{Anomaly, Result};
use serde_json::{Value, json};
use super::{MetricPublisher, points::{Destination, Point, Points}};

pub(super) struct Azure { client: reqwest::Client, endpoint: url::Url, key: String, memory: Arc<MemoryBudget>, _reservation: Arc<MemoryReservation> }
impl Azure {
    pub fn open(connection: &str, memory: Arc<MemoryBudget>) -> Result<Self> {
        if connection.encode_utf16().count() > 4096 { return Err(invalid("Application Insights connection string exceeds 4096 characters")); }
        let reservation = memory.reserve(65_536 + connection.len() * 32)?;
        let (key, endpoint) = connection_parts(connection)?;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).redirect(reqwest::redirect::Policy::none()).build().map_err(|_| invalid("Could not initialize Azure Monitor HTTP client"))?;
        Ok(Self { client, key, endpoint, memory, _reservation: reservation })
    }
    async fn send(&self, points: &[Point], timestamp: i64) -> Result<()> {
        let mut selected = (0..points.len()).collect::<Vec<_>>();
        let mut rejected = 0usize;
        for attempt in 0..4 {
            let payload = payload(points, &selected, &self.key, timestamp, &self.memory)?;
            let response = self.client.post(self.endpoint.clone()).header("content-type", "application/x-json-stream").body(payload).send().await;
            let mut delay = Duration::from_millis(1000u64 << attempt);
            match response {
                Ok(mut response) => {
                    let status = response.status().as_u16();
                    if status == 206 {
                        let (report, _response_charge) = read_response(&mut response, &self.memory).await?;
                        let errors = report.get("errors").and_then(Value::as_array).ok_or_else(|| invalid("Azure Monitor partial response has no error list"))?;
                        let mut retry = Vec::with_capacity(selected.len());
                        let mut seen = BTreeSet::new();
                        for error in errors {
                            let index = error.get("index").and_then(Value::as_u64).and_then(|index| usize::try_from(index).ok()).filter(|index| *index < selected.len()).ok_or_else(|| invalid("Azure Monitor partial response has an invalid metric index"))?;
                            if !seen.insert(index) { return Err(invalid("Azure Monitor partial response repeats a metric index")); }
                            let code = response_code(error)?;
                            if retryable(code) { retry.push(selected[index]); } else { rejected += 1; }
                        }
                        let received = report.get("itemsReceived").and_then(Value::as_u64);
                        let accepted = report.get("itemsAccepted").and_then(Value::as_u64);
                        if received != Some(selected.len() as u64) || accepted != Some((selected.len() - seen.len()) as u64) { return Err(invalid("Azure Monitor partial response counts disagree")); }
                        selected = retry;
                        if selected.is_empty() { return if rejected == 0 { Ok(()) } else { Err(invalid(format!("Azure Monitor rejected {rejected} metric(s)"))) }; }
                    } else if (200..300).contains(&status) {
                        return if rejected == 0 { Ok(()) } else { Err(invalid(format!("Azure Monitor rejected {rejected} metric(s)"))) };
                    } else if !retryable(status) { return Err(invalid(format!("Azure Monitor returned HTTP {status}"))); }
                    if let Some(seconds) = response.headers().get("retry-after").and_then(|value| value.to_str().ok()).and_then(|value| value.parse::<u64>().ok()) { delay = Duration::from_secs(seconds); }
                }
                Err(error) if error.is_builder() => return Err(invalid("Azure Monitor request could not be constructed")),
                Err(_) => (),
            }
            if attempt < 3 { tokio::time::sleep(delay).await; }
        }
        Err(invalid("Azure Monitor export exhausted its retry limit"))
    }
}
#[async_trait::async_trait]
impl MetricPublisher for Azure {
    async fn publish(&self, metrics: &[CatalogMetric], timestamp: i64) -> Result<()> {
        let points = Points::new(metrics, Destination::Azure, &self.memory)?;
        let mut failure = None;
        for chunk in points.values.chunks(500) {
            if let Err(error) = tokio::time::timeout(Duration::from_secs(30), self.send(chunk, timestamp)).await.unwrap_or_else(|_| Err(invalid("Azure Monitor export deadline expired"))) { if failure.is_none() { failure = Some(error); } }
        }
        failure.map_or(Ok(()), Err)
    }
}

fn connection_parts(connection: &str) -> Result<(String, url::Url)> {
    let mut exact = BTreeSet::new();
    let mut pairs = BTreeMap::new();
    for part in connection.split(';').map(str::trim).filter(|part| !part.is_empty()) {
        let (key, value) = part.split_once('=').filter(|(_, value)| !value.contains('=')).ok_or_else(|| invalid("Invalid Application Insights connection string"))?;
        if !exact.insert(key) { return Err(invalid("Application Insights connection string repeats a key")); }
        pairs.insert(key.to_ascii_lowercase(), value);
    }
    let key = pairs.get("instrumentationkey").filter(|key| !key.is_empty()).ok_or_else(|| invalid("Application Insights connection string requires InstrumentationKey"))?.to_string();
    let mut endpoint = "https://dc.services.visualstudio.com".to_owned();
    if let Some(suffix) = pairs.get("endpointsuffix").filter(|suffix| !suffix.is_empty()) { endpoint = format!("https://dc.{}", suffix.trim_matches('.')); }
    for name in ["ingestionendpoint", "liveendpoint", "profilerendpoint", "snapshotendpoint"] {
        if let Some(value) = pairs.get(name).filter(|value| !value.is_empty()) { validate_endpoint(value)?; if name == "ingestionendpoint" { endpoint = value.to_string(); } }
    }
    let endpoint = format!("{}/v2/track", endpoint.trim_end_matches('/'));
    Ok((key, validate_endpoint(&endpoint)?))
}
fn validate_endpoint(value: &str) -> Result<url::Url> {
    let value = url::Url::parse(value).map_err(|_| invalid("Invalid Application Insights endpoint"))?;
    if !matches!(value.scheme(), "http" | "https") || value.host_str().is_none() { return Err(invalid("Application Insights endpoint requires HTTP or HTTPS and a host")); }
    Ok(value)
}
struct Payload { bytes: Vec<u8>, _reservation: Arc<MemoryReservation> }
impl AsRef<[u8]> for Payload { fn as_ref(&self) -> &[u8] { &self.bytes } }
fn payload(points: &[Point], selected: &[usize], key: &str, timestamp: i64, memory: &Arc<MemoryBudget>) -> Result<bytes::Bytes> {
    let size = selected.iter().try_fold(4096usize, |size, index| {
        let point = &points[*index];
        let strings = point.tags.iter().try_fold(point.name.len() + key.len() + 2048, |size, (key, value)| size.checked_add(key.len()).and_then(|size| size.checked_add(value.len())).and_then(|size| size.checked_add(256)).ok_or_else(overflow))?;
        size.checked_add(strings.checked_mul(32).ok_or_else(overflow)?).ok_or_else(overflow)
    })?;
    let reservation = memory.reserve(size)?;
    let time = chrono::DateTime::from_timestamp_millis(timestamp).ok_or_else(|| invalid("Metric export timestamp is outside the UTC range"))?.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let normalized = if key.chars().all(|character| matches!(character, '.' | '-' | ' ')) { String::new() } else { format!("{}.", key.replace('-', "").to_lowercase()) };
    let name = format!("Microsoft.ApplicationInsights.{normalized}Metric");
    let mut bytes = Vec::new();
    for index in selected {
        let point = &points[*index];
        let mut data = json!({"name": point.name, "kind": if point.aggregate.is_some() { 1 } else { 0 }, "value": point.value});
        if let Some((count, maximum)) = point.aggregate { data["count"] = json!(count); data["min"] = json!(0.0); data["max"] = json!(maximum); }
        let envelope = json!({"ver":1,"name":name,"time":time,"sampleRate":100.0,"iKey":key,"tags":{"ai.internal.sdkVersion":concat!("rust:rtdb/",env!("CARGO_PKG_VERSION"))},"data":{"baseType":"MetricData","baseData":{"ver":2,"metrics":[data],"properties":point.tags}}});
        if !bytes.is_empty() { bytes.push(b'\n'); }
        serde_json::to_writer(&mut bytes, &envelope).map_err(|_| invalid("Could not encode Azure Monitor metric"))?;
    }
    Ok(bytes::Bytes::from_owner(Payload { bytes, _reservation: reservation }))
}
async fn read_response(response: &mut reqwest::Response, memory: &Arc<MemoryBudget>) -> Result<(Value, Arc<MemoryReservation>)> {
    const MAX: usize = 65_536;
    let reservation = memory.reserve(MAX * 32)?;
    if response.content_length().is_some_and(|length| length > MAX as u64) { return Err(invalid("Azure Monitor response exceeds its byte limit")); }
    let mut bytes = Vec::with_capacity(MAX);
    while let Some(chunk) = response.chunk().await.map_err(|_| invalid("Could not read Azure Monitor response"))? {
        if chunk.len() > MAX - bytes.len() { return Err(invalid("Azure Monitor response exceeds its byte limit")); }
        bytes.extend_from_slice(&chunk);
    }
    Ok((serde_json::from_slice(&bytes).map_err(|_| invalid("Azure Monitor returned invalid JSON"))?, reservation))
}
fn retryable(status: u16) -> bool { matches!(status, 408 | 429 | 439 | 500 | 502 | 503 | 504) }
fn response_code(error: &Value) -> Result<u16> { error.get("statusCode").and_then(Value::as_u64).and_then(|code| u16::try_from(code).ok()).filter(|code| (100..=599).contains(code)).ok_or_else(|| invalid("Azure Monitor partial response has an invalid HTTP status")) }
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("rtdb.metrics/azure-export", message) }
fn overflow() -> Anomaly { invalid("Azure Monitor payload allocation size overflow") }

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn read_request(stream: &mut tokio::net::TcpStream) -> Vec<Value> {
        let mut bytes = Vec::new();
        let header_end = loop {
            let mut byte = [0u8; 1]; stream.read_exact(&mut byte).await.unwrap(); bytes.push(byte[0]);
            if bytes.ends_with(b"\r\n\r\n") { break bytes.len(); }
            assert!(bytes.len() < 8192);
        };
        let header = std::str::from_utf8(&bytes).unwrap().to_ascii_lowercase();
        let length: usize = header.lines().find_map(|line| line.strip_prefix("content-length:").map(str::trim)).unwrap().parse().unwrap();
        bytes.resize(header_end + length, 0); stream.read_exact(&mut bytes[header_end..]).await.unwrap();
        bytes[header_end..].split(|byte| *byte == b'\n').map(|line| serde_json::from_slice(line).unwrap()).collect()
    }
    #[tokio::test]
    async fn partial_acceptance_retries_only_failed_metric_indices() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("InstrumentationKey=fixture;IngestionEndpoint=http://{}", listener.local_addr().unwrap());
        let server = async {
            let (mut first, _) = listener.accept().await.unwrap(); let original = read_request(&mut first).await;
            let body = r#"{"itemsReceived":2,"itemsAccepted":1,"errors":[{"index":1,"statusCode":503,"message":"retry"}]}"#;
            first.write_all(format!("HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nRetry-After: 0\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap(); drop(first);
            let (mut second, _) = listener.accept().await.unwrap(); let retried = read_request(&mut second).await;
            second.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
            assert_eq!(original.len(), 2); assert_eq!(retried.len(), 1);
            assert_eq!(retried[0]["data"]["baseData"]["metrics"][0]["name"], "second");
        };
        let memory = MemoryBudget::new(1 << 24);
        let publisher = Azure::open(&endpoint, Arc::clone(&memory)).unwrap();
        let metrics = [CatalogMetric::Gauge { name: "first".into(), tags: BTreeMap::new(), unit: None, value: 1.0 }, CatalogMetric::Gauge { name: "second".into(), tags: BTreeMap::new(), unit: None, value: 2.0 }];
        let (result, ()) = tokio::join!(publisher.publish(&metrics, 0), server); result.unwrap();
        drop(publisher); assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn malformed_partial_status_cannot_wrap_into_a_retryable_http_code() {
        for code in [json!(65_536 + 503), json!(-1), json!(503.5), json!("503"), json!(null), json!(600)] { assert!(response_code(&json!({"statusCode":code})).is_err()); }
        assert!(response_code(&json!({})).is_err());
        assert_eq!(response_code(&json!({"statusCode":503})).unwrap(), 503);
    }
    #[test]
    fn connection_string_parsing_tests_preserve_suffix_override_and_path() {
        assert_eq!(connection_parts("InstrumentationKey=abc;EndpointSuffix=.applicationinsights.us.;").unwrap().1.as_str(), "https://dc.applicationinsights.us/v2/track");
        assert_eq!(connection_parts("instrumentationkey=abc;EndpointSuffix=x;IngestionEndpoint=http://localhost:123/prefix/").unwrap().1.as_str(), "http://localhost:123/prefix/v2/track");
        assert!(connection_parts("InstrumentationKey=a;InstrumentationKey=b").is_err());
        assert!(connection_parts("InstrumentationKey=a;LiveEndpoint=ftp://x").is_err());
        assert!(connection_parts("Authorization=ikey").is_err());
    }
    #[test]
    fn metric_telemetry_aggregation_envelope_and_payload_lease() {
        let memory = MemoryBudget::new(1 << 20);
        let point = Point { name: "query_timer".into(), tags: BTreeMap::from([("node-id".into(), "n".into())]), value: 4.0, unit: super::super::points::Unit::Milliseconds, aggregate: Some((2, 3.0)) };
        let bytes = payload(&[point], &[0], "Ab-cd", 0, &memory).unwrap();
        let parsed: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed["name"], "Microsoft.ApplicationInsights.abcd.Metric");
        assert_eq!(parsed["data"]["baseData"]["metrics"][0]["count"], 2);
        assert_eq!(parsed["data"]["baseData"]["metrics"][0]["min"], 0.0);
        let clone = bytes.clone(); drop(bytes); assert!(memory.used_bytes() > 0); drop(clone); assert_eq!(memory.used_bytes(), 0);
    }
}
