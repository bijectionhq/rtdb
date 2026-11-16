use rtdb_types::{Anomaly, AnomalyCategory};
use tonic::{Code, Status};
use serde::Serialize;

#[derive(Serialize)]
struct AnomalyWire<'a> {
    category: AnomalyCategory,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<&'a str>,
    message: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<&'a std::collections::BTreeMap<String, serde_json::Value>>,
}
impl<'a> From<&'a Anomaly> for AnomalyWire<'a> {
    fn from(error: &'a Anomaly) -> Self { Self { category: error.category, code: (!error.code.is_empty()).then_some(error.code.as_str()), message: &error.message, data: (!error.context.is_empty()).then_some(&error.context) } }
}
struct MetadataBytes(Vec<u8>);
impl std::io::Write for MetadataBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > 4096 - self.0.len() { return Err(std::io::Error::other("Anomaly metadata exceeds 4 KiB")); }
        self.0.extend_from_slice(bytes); Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

pub(crate) fn status(error: Anomaly) -> Status {
    let code = match error.category {
        AnomalyCategory::Incorrect => Code::InvalidArgument,
        AnomalyCategory::Forbidden => Code::Unauthenticated,
        AnomalyCategory::NotFound => Code::NotFound,
        AnomalyCategory::Conflict => Code::InvalidArgument,
        AnomalyCategory::Busy => Code::ResourceExhausted,
        AnomalyCategory::Unsupported => Code::Unimplemented,
        AnomalyCategory::Unavailable => Code::Unavailable,
        AnomalyCategory::Interrupted => Code::Cancelled,
        AnomalyCategory::Fault => Code::Internal,
    };
    let mut result = Status::new(code, error.message.clone());
    let mut bytes = MetadataBytes(Vec::with_capacity(4096));
    if serde_json::to_writer(&mut bytes, &AnomalyWire::from(&error)).is_ok() { result.metadata_mut().insert_bin("xtdb-anomaly-bin", tonic::metadata::MetadataValue::from_bytes(&bytes.0)); }
    result
}
pub(crate) fn anomaly_json(error: &Anomaly) -> serde_json::Value {
    serde_json::to_value(AnomalyWire::from(error)).expect("anomaly JSON")
}
pub(crate) fn allocation_bound(error: &Anomaly) -> Result<usize, Status> {
    let mut bytes = crate::wire::add(4096, crate::wire::mul(crate::wire::add(error.code.len(), error.message.len())?, 16)?)?;
    for (key, value) in &error.context { bytes = crate::wire::add(bytes, crate::wire::add(crate::wire::mul(key.len(), 16)?, json_bound(value)?)?)?; }
    Ok(bytes)
}
fn json_bound(value: &serde_json::Value) -> Result<usize, Status> {
    match value {
        serde_json::Value::String(value) => crate::wire::add(256, crate::wire::mul(value.len(), 16)?),
        serde_json::Value::Array(values) => values.iter().try_fold(256, |bytes, value| crate::wire::add(bytes, json_bound(value)?)),
        serde_json::Value::Object(values) => values.iter().try_fold(256, |bytes, (key, value)| crate::wire::add(bytes, crate::wire::add(crate::wire::mul(key.len(), 16)?, json_bound(value)?)?)),
        _ => Ok(256),
    }
}
pub(crate) fn invalid(message: impl Into<String>) -> Status { Status::invalid_argument(message) }
pub(crate) fn cancelled() -> Status { Status::cancelled("Flight call cancelled") }
pub(crate) fn arrow(error: impl std::fmt::Display) -> Status { Status::invalid_argument(error.to_string()) }
pub(crate) fn unsupported() -> Status { Status::unimplemented("Flight operation is not supported") }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn caller_categories_and_bounded_anomaly_metadata_match_flight_mapping() {
        let error = Anomaly::new(AnomalyCategory::Conflict, "xtdb/conflict", "conflict").with_context("key", 42.into());
        let status = status(error); assert_eq!(status.code(), Code::InvalidArgument);
        let bytes = status.metadata().get_bin("xtdb-anomaly-bin").unwrap().to_bytes().unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap(); assert_eq!(value["category"], "conflict"); assert_eq!(value["data"]["key"], 42);
        assert_eq!(super::status(Anomaly::new(AnomalyCategory::Forbidden, "xtdb/auth", "denied")).code(), Code::Unauthenticated);
        let oversized = Anomaly::incorrect("xtdb/error", "bad request").with_context("large", "x".repeat(4097).into());
        assert!(super::status(oversized).metadata().get_bin("xtdb-anomaly-bin").is_none());
    }
}
