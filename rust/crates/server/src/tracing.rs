use std::sync::Arc;
use std::time::Duration;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_metrics::{TraceReceiver, TraceRecord, TraceRecorder};
use rtdb_types::{Anomaly, Result};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct TraceExportHandle { recorder: Arc<TraceRecorder>, drain: CancellationToken }
impl TraceExportHandle {
    pub fn recorder(&self) -> &Arc<TraceRecorder> { &self.recorder }
    /// Stops accepting spans and gives already admitted exports a bounded drain period.
    pub fn close(&self) { self.recorder.close(); self.drain.cancel(); }
}
pub struct TraceExporter {
    handle: TraceExportHandle, receiver: TraceReceiver, client: reqwest::Client,
    endpoint: url::Url, service: String, memory: Arc<MemoryBudget>, _reservation: Arc<MemoryReservation>,
}
impl TraceExporter {
    /// Construction starts no work; the owner starts `run`, then closes admission before joining it.
    pub fn open(settings: &crate::config::TracingSettings, memory: Arc<MemoryBudget>) -> Result<(TraceExportHandle, Self)> {
        let reservation = memory.reserve(settings.endpoint.len().checked_add(settings.service_name.len()).and_then(|bytes| bytes.checked_mul(8)).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(size_error)?)?;
        let endpoint = url::Url::parse(&settings.endpoint).map_err(|_| invalid("Invalid OTLP endpoint"))?;
        if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() { return Err(invalid("OTLP endpoint must be an HTTP or HTTPS URL")); }
        let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).redirect(reqwest::redirect::Policy::none()).build().map_err(|_| invalid("Failed to initialize OTLP HTTP client"))?;
        let (recorder, receiver) = TraceRecorder::new(Arc::clone(&memory), 1024, settings.query_tracing, settings.transaction_tracing)?;
        let handle = TraceExportHandle { recorder, drain: CancellationToken::new() };
        Ok((handle.clone(), Self { handle, receiver, client, endpoint, service: settings.service_name.clone(), memory, _reservation: reservation }))
    }
    pub async fn run(mut self) {
        let drain = self.handle.drain.clone();
        let work = self.export_all(); tokio::pin!(work);
        tokio::select! {
            _ = &mut work => (),
            _ = drain.cancelled() => { if tokio::time::timeout(Duration::from_secs(30), &mut work).await.is_err() { ::tracing::warn!("Trace export drain timed out"); } }
        }
    }
    async fn export_all(&mut self) {
        while let Some(record) = self.receiver.next().await {
            match self.export(&record).await {
                Ok(()) => self.receiver.exported(),
                Err(error) => { self.receiver.failed(); ::tracing::warn!("Trace export failed: {error}"); }
            }
        }
    }
    async fn export(&self, record: &TraceRecord) -> Result<()> {
        let payload = record.encode(&self.service, &self.memory)?.into_bytes();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let work = async {
            for attempt in 0..5 {
                let response = self.client.post(self.endpoint.clone()).header("content-type", "application/json").body(payload.clone()).send().await;
                let mut delay = Duration::from_millis(1000u64 << attempt);
                match response {
                    Ok(mut response) => {
                        let status = response.status();
                        if status.is_success() {
                            const MAX_RESPONSE: usize = 65_536;
                            if response.content_length().is_some_and(|bytes| bytes > MAX_RESPONSE as u64) { return Err(invalid("OTLP response exceeds its byte limit")); }
                            let _reservation = self.memory.reserve(MAX_RESPONSE * 32)?;
                            let mut body = Vec::with_capacity(MAX_RESPONSE);
                            while let Some(chunk) = response.chunk().await.map_err(|_| unavailable())? {
                                if chunk.len() > MAX_RESPONSE - body.len() { return Err(invalid("OTLP response exceeds its byte limit")); }
                                body.extend_from_slice(&chunk);
                            }
                            if body.is_empty() { return Ok(()); }
                            let response: serde_json::Value = serde_json::from_slice(&body).map_err(|_| invalid("OTLP endpoint returned invalid JSON"))?;
                            if let Some(partial) = response.get("partialSuccess") {
                                let rejected = partial.get("rejectedSpans").and_then(|value| value.as_u64().or_else(|| value.as_str().and_then(|value| value.parse().ok()))).unwrap_or(0);
                                let message = partial.get("errorMessage").and_then(serde_json::Value::as_str).unwrap_or("");
                                if rejected > 0 { return Err(invalid(format!("OTLP rejected {rejected} span(s): {message}"))); }
                                if !message.is_empty() { ::tracing::warn!("OTLP export warning: {message}"); }
                            }
                            return Ok(());
                        }
                        if ![429, 502, 503, 504].contains(&status.as_u16()) { return Err(invalid(format!("OTLP endpoint returned HTTP {}", status.as_u16()))); }
                        if let Some(seconds) = response.headers().get("retry-after").and_then(|value| value.to_str().ok()).and_then(|value| value.parse::<u64>().ok()) { delay = Duration::from_secs(seconds); }
                    }
                    Err(error) if error.is_builder() => return Err(invalid("OTLP request could not be constructed")),
                    Err(_) => (),
                }
                if attempt == 4 { return Err(unavailable()); }
                tokio::time::sleep(delay).await;
            }
            Err(unavailable())
        };
        tokio::time::timeout_at(deadline, work).await.map_err(|_| unavailable())?
    }
}
impl Drop for TraceExporter { fn drop(&mut self) { self.handle.close(); } }
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("rtdb.tracing/export", message) }
fn unavailable() -> Anomaly { Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.tracing/unavailable", "OTLP endpoint was unavailable before the export deadline") }
fn size_error() -> Anomaly { invalid("Trace exporter allocation size overflow") }
