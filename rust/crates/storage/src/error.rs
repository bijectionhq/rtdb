use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, Error>;

/// The cutoff a [`crate::ReadLease`] passed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseCutoff {
    /// A deadline on the caller's monotonic clock, such as an owned read's validation deadline.
    Clocked,
    /// A provider's serving cutoff.
    Provider,
}

impl std::fmt::Display for LeaseCutoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self { Self::Clocked => "clocked", Self::Provider => "provider" })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid object key: {0}")]
    InvalidKey(String),
    /// XTDB's local and in-memory object stores report a missing object in these words.
    #[error("Object {0} doesn't exist.")]
    MissingObject(String),
    #[error("range [{start}, {end}) exceeds object {key} of length {length}")]
    InvalidRange { key: String, start: u64, end: u64, length: u64 },
    #[error("different bytes already occupy immutable object {0}")]
    ConflictingObject(String),
    #[error("storage task failed: Immutable object {key} has a different length from {length}")]
    ImmutableLength { key: String, length: usize },
    #[error("I/O at {path}: {source}")]
    Io { path: PathBuf, #[source] source: std::io::Error },
    #[error("invalid local log at {path}, byte {offset}: {reason}")]
    CorruptLog { path: PathBuf, offset: u64, reason: String },
    #[error("requested log position {requested} is unavailable; first retained position is {first_retained}")]
    UnavailableLogRange { requested: i64, first_retained: i64 },
    #[error("log position {requested} is durable through {tip} and the fetch is still pending")]
    LogFetchPending { requested: i64, tip: i64 },
    #[error("log is closed: {0}")]
    Closed(String),
    #[error("cannot append to read-only log {0}")]
    ReadOnlyLog(PathBuf),
    #[error("log range exceeds read limits of {maximum_records} records or {maximum_payload_bytes} payload bytes")]
    LogReadLimit { maximum_records: usize, maximum_payload_bytes: usize },
    #[error("message has {size} bytes; maximum is {maximum}")]
    MessageTooLarge { size: usize, maximum: usize },
    #[error("the journal has no ordinary capacity for {required_bytes} charged bytes")]
    AppendCapacity { required_bytes: u64 },
    #[error("the journal writer cannot enqueue the staged record inside its guarded cutoff")]
    GuardedAppendCutoff,
    #[error("the journal writer cannot enqueue the staged record without an idle prefix on a live write quorum")]
    GuardedAppendUnavailable,
    #[error("the journal provider does not implement guarded append")]
    GuardedAppendUnsupported,
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
    #[error("the held view requires an erased storage key")]
    ErasedUnderView,
    #[error("the read lease expired at its {cutoff} cutoff")]
    ReadLeaseExpired { cutoff: LeaseCutoff },
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
        use rtdb_types::{Anomaly, AnomalyCategory as Category, AnomalyKind};
        let message = error.to_string();
        let kind = match &error {
            Error::MemoryLimit { .. } => Some(AnomalyKind::ResourceExhausted),
            Error::MessageTooLarge { .. } => Some(AnomalyKind::ConfigurationLimitExceeded),
            Error::MissingObject(_) => Some(AnomalyKind::MissingObject),
            Error::InvalidRange { .. } => Some(AnomalyKind::CorruptObject),
            Error::ReadLeaseExpired { cutoff: LeaseCutoff::Clocked } => Some(AnomalyKind::ReadExpired),
            _ => None,
        };
        let (category, code, context) = match error {
            Error::InvalidKey(key) => (Category::Incorrect, "rtdb.storage/invalid-key", serde_json::json!({"key": key})),
            Error::MissingObject(key) => (Category::NotFound, "rtdb.storage/missing-object", serde_json::json!({"key": key})),
            Error::InvalidRange { key, start, end, length } => (Category::Incorrect, "rtdb.storage/invalid-range", serde_json::json!({"key": key, "start": start, "end": end, "length": length})),
            Error::ConflictingObject(key) => (Category::Conflict, "rtdb.storage/immutable-object-conflict", serde_json::json!({"key": key})),
            Error::Io { path, source } if source.kind() == std::io::ErrorKind::PermissionDenied => (Category::Forbidden, "rtdb.storage/permission-denied", serde_json::json!({"path": path})),
            Error::Io { path, .. } => (Category::Unavailable, "rtdb.storage/io", serde_json::json!({"path": path})),
            Error::CorruptLog { path, offset, .. } => (Category::Fault, "rtdb.storage/corrupt-log", serde_json::json!({"path": path, "offset": offset})),
            Error::UnavailableLogRange { requested, first_retained } => (Category::Unavailable, "rtdb.storage/log-range-unavailable", serde_json::json!({"requested": requested, "first-retained": first_retained})),
            Error::LogFetchPending { requested, tip } => (Category::Unavailable, "rtdb.storage/log-fetch-pending", serde_json::json!({"requested": requested, "tip": tip})),
            Error::Closed(_) => (Category::Unavailable, "rtdb.storage/closed", serde_json::json!({})),
            Error::ReadOnlyLog(path) => (Category::Incorrect, "rtdb.storage/read-only-log", serde_json::json!({"path": path})),
            Error::LogReadLimit { maximum_records, maximum_payload_bytes } => (Category::Busy, "rtdb.storage/log-read-limit", serde_json::json!({"maximum-records": maximum_records, "maximum-payload-bytes": maximum_payload_bytes})),
            Error::MessageTooLarge { size, maximum } => (Category::Incorrect, "rtdb.storage/message-too-large", serde_json::json!({"size": size, "maximum": maximum})),
            Error::AppendCapacity { required_bytes } => (Category::Busy, "rtdb.storage/append-capacity", serde_json::json!({"required-bytes": required_bytes})),
            Error::GuardedAppendCutoff => (Category::Unavailable, "rtdb.storage/guarded-append-cutoff", serde_json::json!({})),
            Error::GuardedAppendUnavailable => (Category::Unavailable, "rtdb.storage/guarded-append-unavailable", serde_json::json!({})),
            Error::GuardedAppendUnsupported => (Category::Unsupported, "rtdb.storage/guarded-append-unsupported", serde_json::json!({})),
            Error::MemoryLimit { requested, limit, retained } => (Category::Busy, "rtdb.storage/memory-limit", serde_json::json!({"requested": requested, "limit": limit, "retained": retained})),
            Error::AudienceWorkLimit => (Category::Busy, "rtdb.storage/audience-work-limit", serde_json::json!({})),
            Error::Task(_) | Error::ImmutableLength { .. } => (Category::Fault, "rtdb.storage/task", serde_json::json!({})),
            Error::Remote { key, .. } => (Category::Unavailable, "rtdb.storage/remote", serde_json::json!({"key": key})),
            Error::Deposed { epoch, promised } => (Category::Conflict, "rtdb.storage/deposed", serde_json::json!({"epoch": epoch, "promised": promised})),
            Error::PermissionDenied { key, .. } => (Category::Forbidden, "rtdb.storage/permission-denied", serde_json::json!({"key": key})),
            Error::ErasedUnderView => (Category::Forbidden, "rtdb/erased-under-view", serde_json::json!({})),
            Error::ReadLeaseExpired { cutoff: LeaseCutoff::Clocked } => (Category::Unavailable, "rtdb.storage/read-lease-expired", serde_json::json!({})),
            // The provider withdrew the serving view, as when a placement moves; the read is closed rather than late.
            Error::ReadLeaseExpired { cutoff: LeaseCutoff::Provider } => (Category::Unavailable, "rtdb.storage/closed", serde_json::json!({})),
            Error::MultipartAbort { upload, abort } => {
                let mut anomaly = Anomaly::from(*upload);
                anomaly.message = Some(message);
                anomaly.context.insert("multipart-abort".into(), serde_json::json!(Anomaly::from(*abort)));
                return anomaly;
            }
        };
        let mut anomaly = Anomaly::new(category, code, message);
        anomaly.kind = kind;
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

    #[test]
    fn a_missing_object_reports_xtdbs_text_and_its_kind() {
        let missing = rtdb_types::Anomaly::from(Error::MissingObject("tables/public$foo/data/l00-rc-b00.arrow".into()));
        assert_eq!(missing.message.as_deref(), Some("Object tables/public$foo/data/l00-rc-b00.arrow doesn't exist."));
        assert!(missing.is(rtdb_types::AnomalyKind::MissingObject));
        assert_eq!(rtdb_types::AnomalyKind::from_wire_code(&missing.code), Some(rtdb_types::AnomalyKind::MissingObject));
        let short = rtdb_types::Anomaly::from(Error::InvalidRange { key: "k".into(), start: 0, end: 9, length: 4 });
        assert!(short.is(rtdb_types::AnomalyKind::CorruptObject));
    }

    #[test]
    fn only_the_memory_limit_is_resource_exhaustion() {
        let exhausted = rtdb_types::Anomaly::from(Error::MemoryLimit { requested: 2, limit: 1, retained: 0 });
        assert!(exhausted.is(rtdb_types::AnomalyKind::ResourceExhausted));
        assert_eq!(exhausted.category, rtdb_types::AnomalyCategory::Busy);
        assert!(!rtdb_types::Anomaly::from(Error::AudienceWorkLimit).is(rtdb_types::AnomalyKind::ResourceExhausted));
    }
}
