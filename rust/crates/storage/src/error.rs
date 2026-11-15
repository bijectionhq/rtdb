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
    #[error("message has {size} bytes; maximum is {maximum}")]
    MessageTooLarge { size: usize, maximum: usize },
    #[error("cache requires {requested} bytes; limit is {limit} with {retained} retained")]
    MemoryLimit { requested: usize, limit: usize, retained: usize },
    #[error("storage task failed: {0}")]
    Task(String),
    #[error("remote storage at {key}: {message}")]
    Remote { key: String, message: String },
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
            Error::Io { path, .. } => (Category::Unavailable, "rtdb.storage/io", serde_json::json!({"path": path})),
            Error::CorruptLog { path, offset, .. } => (Category::Fault, "rtdb.storage/corrupt-log", serde_json::json!({"path": path, "offset": offset})),
            Error::UnavailableLogRange { requested, first_retained } => (Category::Unavailable, "rtdb.storage/log-range-unavailable", serde_json::json!({"requested": requested, "first-retained": first_retained})),
            Error::Closed(_) => (Category::Unavailable, "rtdb.storage/closed", serde_json::json!({})),
            Error::MessageTooLarge { size, maximum } => (Category::Incorrect, "rtdb.storage/message-too-large", serde_json::json!({"size": size, "maximum": maximum})),
            Error::MemoryLimit { requested, limit, retained } => (Category::Busy, "rtdb.storage/memory-limit", serde_json::json!({"requested": requested, "limit": limit, "retained": retained})),
            Error::Task(_) => (Category::Fault, "rtdb.storage/task", serde_json::json!({})),
            Error::Remote { key, .. } => (Category::Unavailable, "rtdb.storage/remote", serde_json::json!({"key": key})),
        };
        let mut anomaly = Anomaly::new(category, code, message);
        if let serde_json::Value::Object(context) = context { anomaly.context.extend(context); }
        anomaly
    }
}
