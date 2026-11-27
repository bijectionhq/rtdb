use rtdb_format::diagnostics::JsonAnomaly;
use rtdb_types::{Anomaly, AnomalyCategory};
use tonic::{Code, Status};
struct MetadataBytes(Vec<u8>);
impl std::io::Write for MetadataBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > 4096 - self.0.len() { return Err(std::io::Error::other("Anomaly metadata exceeds 4 KiB")); }
        self.0.extend_from_slice(bytes); Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

pub(crate) fn status(error: Anomaly) -> Status { status_ref(&error) }

fn status_ref(error: &Anomaly) -> Status {
    let code = match error.category {
        AnomalyCategory::Incorrect => Code::InvalidArgument,
        AnomalyCategory::Forbidden => Code::PermissionDenied,
        AnomalyCategory::NotFound => Code::NotFound,
        AnomalyCategory::Conflict => Code::InvalidArgument,
        AnomalyCategory::Busy => Code::ResourceExhausted,
        AnomalyCategory::Unsupported => Code::Unimplemented,
        AnomalyCategory::Unavailable => Code::Unavailable,
        AnomalyCategory::Interrupted => Code::Cancelled,
        AnomalyCategory::Fault => Code::Internal,
    };
    let mut result = Status::new(code, description(error));
    let mut bytes = MetadataBytes(Vec::with_capacity(4096));
    if serde_json::to_writer(&mut bytes, &JsonAnomaly::from(error)).is_ok() { result.metadata_mut().insert_bin("xtdb-anomaly-bin", tonic::metadata::MetadataValue::from_bytes(&bytes.0)); }
    result
}
pub(crate) fn anomaly_json(error: &Anomaly) -> Result<serde_json::Value, Status> {
    serde_json::to_value(JsonAnomaly::from(error)).map_err(|error| Status::internal(error.to_string()))
}
pub(crate) fn allocation_bound(error: &Anomaly) -> Result<usize, Status> {
    let mut bytes = crate::wire::add(4096, crate::wire::mul(crate::wire::add(error.code.len(), description(error).len())?, 16)?)?;
    for (key, value) in &error.context { bytes = crate::wire::add(bytes, crate::wire::add(crate::wire::mul(key.len(), 16)?, json_bound(value)?)?)?; }
    Ok(bytes)
}
fn description(error: &Anomaly) -> &str {
    error.message.as_deref().unwrap_or(match error.category {
        AnomalyCategory::Incorrect => "xtdb.api.error.Incorrect",
        AnomalyCategory::Unsupported => "xtdb.api.error.Unsupported",
        AnomalyCategory::NotFound => "xtdb.api.error.NotFound",
        AnomalyCategory::Forbidden => "xtdb.api.error.Forbidden",
        AnomalyCategory::Conflict => "xtdb.api.error.Conflict",
        AnomalyCategory::Busy => "xtdb.api.error.Busy",
        AnomalyCategory::Unavailable => "xtdb.api.error.Unavailable",
        AnomalyCategory::Interrupted => "xtdb.api.error.Interrupted",
        AnomalyCategory::Fault => "xtdb.api.error.Fault",
    })
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
        let forbidden = super::status(Anomaly::new(AnomalyCategory::Forbidden, "xtdb/auth", "denied"));
        assert_eq!(forbidden.code(), Code::PermissionDenied);
        let forbidden_bytes = forbidden.metadata().get_bin("xtdb-anomaly-bin").unwrap().to_bytes().unwrap();
        let forbidden_value: serde_json::Value = serde_json::from_slice(&forbidden_bytes).unwrap();
        assert_eq!(forbidden_value["category"], "forbidden");
        assert_eq!(forbidden_value["code"], "xtdb/auth");
        let oversized = Anomaly::incorrect("xtdb/error", "bad request").with_context("large", "x".repeat(4097).into());
        assert!(super::status(oversized).metadata().get_bin("xtdb-anomaly-bin").is_none());
    }

    #[test]
    fn absent_messages_use_category_class_names_and_are_omitted_from_metadata() {
        for (category, class) in [(AnomalyCategory::Incorrect, "Incorrect"), (AnomalyCategory::Unsupported, "Unsupported"), (AnomalyCategory::NotFound, "NotFound"), (AnomalyCategory::Forbidden, "Forbidden"), (AnomalyCategory::Conflict, "Conflict"), (AnomalyCategory::Busy, "Busy"), (AnomalyCategory::Unavailable, "Unavailable"), (AnomalyCategory::Interrupted, "Interrupted"), (AnomalyCategory::Fault, "Fault")] {
            let error = Anomaly::new_nullable(category, "test/error", None);
            let json = anomaly_json(&error).unwrap();
            assert!(json.get("message").is_none());
            let bound = allocation_bound(&error).unwrap();
            let status = status(error);
            assert_eq!(status.message(), format!("xtdb.api.error.{class}"));
            assert!(bound >= 4096 + status.message().len() * 16);
            let bytes = status.metadata().get_bin("xtdb-anomaly-bin").unwrap().to_bytes().unwrap();
            assert_eq!(serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(), json);
        }
        let error = Anomaly::fault("test/error", "");
        assert_eq!(anomaly_json(&error).unwrap()["message"], "");
        assert_eq!(status(error).message(), "");
    }

    #[test]
    fn optional_message_metadata_obeys_exact_size_boundary() {
        for message in [None, Some(String::new())] {
            let mut error = Anomaly::new_nullable(AnomalyCategory::Fault, "test/error", message).with_context("padding", "".into());
            let overhead = serde_json::to_vec(&JsonAnomaly::from(&error)).unwrap().len();
            error.context.insert("padding".into(), "x".repeat(4096 - overhead).into());
            let result = status(error.clone());
            assert_eq!(result.metadata().get_bin("xtdb-anomaly-bin").unwrap().to_bytes().unwrap().len(), 4096);
            error.context.insert("padding".into(), "x".repeat(4097 - overhead).into());
            assert!(status(error).metadata().get_bin("xtdb-anomaly-bin").is_none());
        }
    }
}
