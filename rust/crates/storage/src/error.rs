use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid object key: {0}")]
    InvalidKey(String),
    #[error("object does not exist: {0}")]
    MissingObject(String),
    #[error("range [{start}, {end}) exceeds object {key} of length {length}")]
    InvalidRange { key: String, start: u64, end: u64, length: u64 },
    #[error("different bytes already occupy immutable object {0}")]
    ConflictingObject(String),
    #[error("I/O at {path}: {source}")]
    Io { path: PathBuf, #[source] source: std::io::Error },
    #[error("invalid local log at {path}, byte {offset}: {reason}")]
    CorruptLog { path: PathBuf, offset: u64, reason: String },
    #[error("requested log position {requested} is unavailable; first retained position is {first_retained}")]
    UnavailableLogRange { requested: i64, first_retained: i64 },
    #[error("log is closed: {0}")]
    Closed(String),
    #[error("cannot append to read-only log {0}")]
    ReadOnlyLog(PathBuf),
    #[error("log range exceeds read limits of {maximum_records} records or {maximum_payload_bytes} payload bytes")]
    LogReadLimit { maximum_records: usize, maximum_payload_bytes: usize },
    #[error("message has {size} bytes; maximum is {maximum}")]
    MessageTooLarge { size: usize, maximum: usize },
    #[error("cache requires {requested} bytes; limit is {limit} with {retained} retained")]
    MemoryLimit { requested: usize, limit: usize, retained: usize },
    #[error("audience read work exceeded its installed allowance")]
    AudienceWorkLimit,
    #[error("storage task failed: {0}")]
    Task(String),
    #[error("remote storage at {key}: {message}")]
    Remote { key: String, message: String },
    #[error("remote storage permission denied at {key}: {message}")]
    PermissionDenied { key: String, message: String },
    #[error("log writer at epoch {epoch} was deposed by a seal at epoch {promised}")]
    Deposed { epoch: u64, promised: u64 },
    #[error("read lease expired at {deadline:?}; now {now}")]
    ReadLeaseExpired { deadline: Option<i64>, now: i64 },
    #[error("{upload}; multipart abort also failed: {abort}")]
    MultipartAbort { #[source] upload: Box<Error>, abort: Box<Error> },
}

impl Error {
    pub(crate) fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io { path: path.into(), source }
    }
}

impl From<Error> for rtdb_types::Anomaly {
    fn from(error: Error) -> Self {
        use rtdb_types::{Anomaly, AnomalyCategory as Category};
        let message = error.to_string();
        let (category, code, context) = match error {
            Error::InvalidKey(key) => (Category::Incorrect, "rtdb.storage/invalid-key", serde_json::json!({"key": key})),
            Error::MissingObject(key) => (Category::NotFound, "rtdb.storage/missing-object", serde_json::json!({"key": key})),
            Error::InvalidRange { key, start, end, length } => (Category::Incorrect, "rtdb.storage/invalid-range", serde_json::json!({"key": key, "start": start, "end": end, "length": length})),
            Error::ConflictingObject(key) => (Category::Conflict, "rtdb.storage/immutable-object-conflict", serde_json::json!({"key": key})),
            Error::Io { path, source } if source.kind() == std::io::ErrorKind::PermissionDenied => (Category::Forbidden, "rtdb.storage/permission-denied", serde_json::json!({"path": path})),
            Error::Io { path, .. } => (Category::Unavailable, "rtdb.storage/io", serde_json::json!({"path": path})),
            Error::CorruptLog { path, offset, .. } => (Category::Fault, "rtdb.storage/corrupt-log", serde_json::json!({"path": path, "offset": offset})),
            Error::UnavailableLogRange { requested, first_retained } => (Category::Unavailable, "rtdb.storage/log-range-unavailable", serde_json::json!({"requested": requested, "first-retained": first_retained})),
            Error::Closed(_) => (Category::Unavailable, "rtdb.storage/closed", serde_json::json!({})),
            Error::ReadOnlyLog(path) => (Category::Incorrect, "rtdb.storage/read-only-log", serde_json::json!({"path": path})),
            Error::LogReadLimit { maximum_records, maximum_payload_bytes } => (Category::Busy, "rtdb.storage/log-read-limit", serde_json::json!({"maximum-records": maximum_records, "maximum-payload-bytes": maximum_payload_bytes})),
            Error::MessageTooLarge { size, maximum } => (Category::Incorrect, "rtdb.storage/message-too-large", serde_json::json!({"size": size, "maximum": maximum})),
            Error::MemoryLimit { requested, limit, retained } => (Category::Busy, "rtdb.storage/memory-limit", serde_json::json!({"requested": requested, "limit": limit, "retained": retained})),
            Error::AudienceWorkLimit => (Category::Busy, "rtdb.storage/audience-work-limit", serde_json::json!({})),
            Error::Task(_) => (Category::Fault, "rtdb.storage/task", serde_json::json!({})),
            Error::Remote { key, .. } => (Category::Unavailable, "rtdb.storage/remote", serde_json::json!({"key": key})),
            Error::Deposed { epoch, promised } => (Category::Conflict, "rtdb.storage/deposed", serde_json::json!({"epoch": epoch, "promised": promised})),
            Error::PermissionDenied { key, .. } => (Category::Forbidden, "rtdb.storage/permission-denied", serde_json::json!({"key": key})),
            Error::ReadLeaseExpired { deadline, now } => (Category::Unavailable, "rtdb.storage/read-lease-expired", serde_json::json!({"deadline": deadline, "now": now})),
            Error::MultipartAbort { upload, abort } => {
                let mut anomaly = Anomaly::from(*upload);
                anomaly.message = Some(message);
                anomaly.context.insert("multipart-abort".into(), serde_json::json!(Anomaly::from(*abort)));
                return anomaly;
            }
        };
        let mut anomaly = Anomaly::new(category, code, message);
        if let serde_json::Value::Object(context) = context { anomaly.context.extend(context); }
        anomaly
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_permission_denials_preserve_the_path_and_typed_category() {
        let path = PathBuf::from("/isolated/meta/trie.arrow");
        let anomaly = rtdb_types::Anomaly::from(Error::io(&path, std::io::Error::new(std::io::ErrorKind::PermissionDenied, "injected access policy")));
        assert_eq!(anomaly.category, rtdb_types::AnomalyCategory::Forbidden);
        assert_eq!(anomaly.code, "rtdb.storage/permission-denied");
        assert_eq!(anomaly.context["path"], serde_json::json!(path));
        let generic = rtdb_types::Anomaly::from(Error::io("/isolated/other", std::io::Error::other("PermissionDenied")));
        assert_eq!(generic.category, rtdb_types::AnomalyCategory::Unavailable);
        assert_eq!(generic.code, "rtdb.storage/io");
    }

    #[test]
    fn failed_abort_preserves_the_upload_classification_and_both_diagnostics() {
        let anomaly = rtdb_types::Anomaly::from(Error::MultipartAbort {
            upload: Box::new(Error::PermissionDenied { key: "meta/trie.arrow".into(), message: "upload denied".into() }),
            abort: Box::new(Error::Remote { key: "meta/trie.arrow".into(), message: "abort unavailable".into() }),
        });
        assert_eq!(anomaly.category, rtdb_types::AnomalyCategory::Forbidden);
        assert_eq!(anomaly.code, "rtdb.storage/permission-denied");
        assert_eq!(anomaly.context["key"], "meta/trie.arrow");
        assert_eq!(anomaly.context["multipart-abort"]["code"], "rtdb.storage/remote");
        assert!(anomaly.message.as_deref().is_some_and(|message| message.contains("upload denied")));
        assert!(anomaly.message.as_deref().is_some_and(|message| message.contains("abort unavailable")));
    }
}
