use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde_json::Value;
use url::Url;

use crate::Secret;

pub enum HttpRequest {
    Get { url: Url, bearer: Option<Secret> },
    Form { url: Url, fields: BTreeMap<String, Secret> },
}

pub struct HttpResponse {
    pub status: u16,
    pub body: Value,
    _reservation: Arc<MemoryReservation>,
}
impl HttpResponse {
    pub fn new(status: u16, body: Value, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bytes = json_bytes(&body)?;
        Ok(Self { status, body, _reservation: memory.reserve(bytes)? })
    }
}

#[async_trait]
pub trait HttpTransport: Send + Sync {
    /// Completes the request on the caller's task; dropping the future cancels the request.
    async fn request(&self, request: HttpRequest) -> Result<HttpResponse>;
}

pub struct NativeHttp {
    client: reqwest::Client,
    memory: Arc<MemoryBudget>,
    maximum_response_bytes: usize,
}
impl NativeHttp {
    /// Limits the decoded response body; TLS and connection-pool buffers are owned by reqwest.
    pub fn new(memory: Arc<MemoryBudget>, timeout: Duration, maximum_response_bytes: usize) -> Result<Self> {
        Self::with_issuer_ca(memory, timeout, maximum_response_bytes, None)
    }

    /// An explicit private issuer trust bundle replaces the platform TLS roots.
    pub fn with_issuer_ca(
        memory: Arc<MemoryBudget>,
        timeout: Duration,
        maximum_response_bytes: usize,
        issuer_ca_pem: Option<&str>,
    ) -> Result<Self> {
        if maximum_response_bytes == 0 { return Err(Anomaly::incorrect("rtdb.auth/http-limit", "OIDC response limit must be positive")); }
        let mut builder = reqwest::Client::builder().timeout(timeout).redirect(reqwest::redirect::Policy::none());
        if let Some(pem) = issuer_ca_pem {
            let invalid = || Anomaly::incorrect("rtdb.auth/issuer-ca", "OIDC issuer CA must be a nonempty PEM certificate bundle");
            if pem.len() > 64 * 1024 { return Err(invalid()); }
            let certs = reqwest::Certificate::from_pem_bundle(pem.as_bytes()).map_err(|_| invalid())?;
            if certs.is_empty() { return Err(invalid()); }
            builder = builder.tls_certs_only(certs).https_only(true);
        }
        let client = builder.build().map_err(http_error)?;
        Ok(Self { client, memory, maximum_response_bytes })
    }
}

#[async_trait]
impl HttpTransport for NativeHttp {
    async fn request(&self, request: HttpRequest) -> Result<HttpResponse> {
        let input_bytes = match &request {
            HttpRequest::Get { url, bearer } => url.as_str().len().checked_add(bearer.as_ref().map_or(0, |value| value.expose().len())).ok_or_else(body_limit)?,
            HttpRequest::Form { url, fields } => fields.iter().try_fold(url.as_str().len(), |bytes, (key, value)| bytes.checked_add(key.len()).and_then(|bytes| bytes.checked_add(value.expose().len())).and_then(|bytes| bytes.checked_add(256)).ok_or_else(body_limit))?,
        };
        let _request_reservation = self.memory.reserve(input_bytes.checked_mul(8).and_then(|bytes| bytes.checked_add(1024)).ok_or_else(body_limit)?)?;
        let request = match request {
            HttpRequest::Get { url, bearer } => {
                let request = self.client.get(url);
                match bearer { Some(token) => request.bearer_auth(token.expose()), None => request }
            }
            HttpRequest::Form { url, fields } => {
                let fields = fields.iter().map(|(name, value)| (name.as_str(), value.expose())).collect::<BTreeMap<_, _>>();
                self.client.post(url).form(&fields)
            }
        };
        let mut response = request.send().await.map_err(http_error)?;
        let status = response.status().as_u16();
        if response.content_length().is_some_and(|length| length > self.maximum_response_bytes as u64) { return Err(body_limit()); }
        let _bytes_reservation = self.memory.reserve(self.maximum_response_bytes)?;
        let mut bytes = Vec::with_capacity(self.maximum_response_bytes);
        while let Some(chunk) = response.chunk().await.map_err(http_error)? {
            if chunk.len() > self.maximum_response_bytes - bytes.len() { return Err(body_limit()); }
            bytes.extend_from_slice(&chunk);
        }
        let decode_bound = bytes.len().checked_mul(256).and_then(|size| size.checked_add(1024)).ok_or_else(body_limit)?;
        let reservation = self.memory.reserve(decode_bound)?;
        let body = serde_json::from_slice(&bytes).map_err(|_| Anomaly::incorrect("rtdb.auth/invalid-response", "OIDC endpoint returned invalid JSON"))?;
        if json_bytes(&body)? > reservation.bytes() { return Err(Anomaly::fault("rtdb.auth/json-allocation", "JSON decoding exceeded reserved allocation")); }
        Ok(HttpResponse { status, body, _reservation: reservation })
    }
}

fn body_limit() -> Anomaly { Anomaly::incorrect("rtdb.auth/response-too-large", "OIDC response exceeds configured byte limit").with_kind(rtdb_types::AnomalyKind::ConfigurationLimitExceeded) }
fn http_error(_: reqwest::Error) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "rtdb.auth/http", "OIDC HTTP request failed") }
fn json_bytes(value: &Value) -> Result<usize> {
    let extra = match value {
        Value::String(value) => value.capacity(),
        Value::Array(values) => values.iter().try_fold(values.capacity() * std::mem::size_of::<Value>(), |bytes, value| bytes.checked_add(json_bytes(value)?).ok_or_else(body_limit))?,
        Value::Object(values) => values.iter().try_fold(values.len() * (std::mem::size_of::<Value>() + 96), |bytes, (key, value)| bytes.checked_add(key.capacity()).and_then(|bytes| bytes.checked_add(json_bytes(value).ok()?)).ok_or_else(body_limit))?,
        _ => 0,
    };
    std::mem::size_of::<Value>().checked_add(extra).ok_or_else(body_limit)
}
