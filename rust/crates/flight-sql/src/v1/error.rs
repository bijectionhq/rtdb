use prost::Message;
use rtdb_types::{Anomaly, AnomalyCategory as Native};
use tonic::{Code, Status, metadata::MetadataValue};

use crate::rtdb::v1::{AnomalyCategory as Category, Error};

pub fn incorrect(message: &str) -> Status {
    error(
        Category::Incorrect,
        "rtdb/invalid-request",
        message,
        None,
        None,
    )
}
pub fn unsupported(feature: &str) -> Status {
    error(
        Category::Unsupported,
        "rtdb/unsupported",
        "Feature is unavailable",
        Some(feature),
        None,
    )
}
pub fn not_found(code: &str, message: &str) -> Status {
    error(Category::NotFound, code, message, None, None)
}
pub fn unavailable(message: &str) -> Status {
    error(
        Category::Unavailable,
        "rtdb/unavailable",
        message,
        None,
        None,
    )
}

fn error(
    category: Category,
    code: &str,
    message: &str,
    feature: Option<&str>,
    retry_after_millis: Option<u64>,
) -> Status {
    as_status(Error {
        category: category as i32,
        code: code.into(),
        message: message.into(),
        feature: feature.map(str::to_owned),
        retry_after_millis,
    })
}

fn as_status(detail: Error) -> Status {
    let grpc = match detail.category() {
        Category::Incorrect => Code::InvalidArgument,
        Category::Unsupported => Code::Unimplemented,
        Category::NotFound => Code::NotFound,
        Category::Forbidden => Code::PermissionDenied,
        Category::Conflict => Code::Aborted,
        Category::Busy => Code::ResourceExhausted,
        Category::Unavailable => Code::Unavailable,
        Category::Interrupted => Code::Cancelled,
        Category::Fault | Category::Unspecified => Code::Internal,
    };
    let mut status = Status::new(grpc, &detail.message);
    status.metadata_mut().insert_bin(
        "rtdb-error-bin",
        MetadataValue::from_bytes(&detail.encode_to_vec()),
    );
    status
}

fn code(category: Category) -> &'static str {
    match category {
        Category::Incorrect => "rtdb/invalid-request",
        Category::Unsupported => "rtdb/unsupported",
        Category::NotFound => "rtdb/not-found",
        Category::Forbidden => "rtdb/forbidden",
        Category::Conflict => "rtdb/conflict",
        Category::Busy => "rtdb/busy",
        Category::Unavailable => "rtdb/unavailable",
        Category::Interrupted => "rtdb/interrupted",
        Category::Fault | Category::Unspecified => "rtdb/fault",
    }
}

pub fn anomaly(native: Anomaly) -> Status {
    as_status(detail(native))
}

pub(super) fn detail(native: Anomaly) -> Error {
    let category = match native.category {
        _ if matches!(
            native.code.as_str(),
            "rtdb/erased-under-view" | "xtdb/unknown-db"
        ) =>
        {
            Category::NotFound
        }
        Native::Incorrect => Category::Incorrect,
        Native::Unsupported => Category::Unsupported,
        Native::NotFound => Category::NotFound,
        Native::Forbidden => Category::Forbidden,
        Native::Conflict => Category::Conflict,
        Native::Busy => Category::Busy,
        Native::Unavailable => Category::Unavailable,
        Native::Interrupted => Category::Interrupted,
        Native::Fault => Category::Fault,
    };
    let stable = match native.code.as_str() {
        "rtdb/erased-under-view" | "rtdb/cursor-expired" | "rtdb/unknown-branch" => {
            native.code.as_str()
        }
        _ => code(category),
    };
    let retry = (category == Category::Busy).then(|| {
        native
            .context
            .get("retry-after-millis")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(100)
    });
    let message = native.message.as_deref().unwrap_or(stable);
    Error {
        category: category as i32,
        code: stable.to_owned(),
        message: message.to_owned(),
        feature: native
            .context
            .get("feature")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        retry_after_millis: retry,
    }
}

#[derive(serde::Deserialize)]
struct FlightAnomaly {
    category: Native,
    code: Option<String>,
    message: Option<String>,
    #[serde(default)]
    data: std::collections::BTreeMap<String, serde_json::Value>,
}

pub fn map_status(status: Status) -> Status {
    if status.metadata().get_bin("rtdb-error-bin").is_some() {
        return status;
    }
    if let Some(bytes) = status.metadata().get_bin("xtdb-anomaly-bin")
        && let Ok(bytes) = bytes.to_bytes()
        && let Ok(native) = serde_json::from_slice::<FlightAnomaly>(&bytes)
    {
        return anomaly(Anomaly {
            category: native.category,
            code: native.code.unwrap_or_default(),
            message: native.message,
            context: native.data,
        });
    }
    let category = match status.code() {
        Code::InvalidArgument | Code::OutOfRange => Category::Incorrect,
        Code::Unimplemented => Category::Unsupported,
        Code::NotFound => Category::NotFound,
        Code::Unauthenticated | Code::PermissionDenied => Category::Forbidden,
        Code::Aborted | Code::AlreadyExists => Category::Conflict,
        Code::ResourceExhausted => Category::Busy,
        Code::Unavailable | Code::DeadlineExceeded => Category::Unavailable,
        Code::Cancelled => Category::Interrupted,
        _ => Category::Fault,
    };
    let mut mapped = error(
        category,
        code(category),
        status.message(),
        None,
        (category == Category::Busy).then_some(100),
    );
    for key in ["set-cookie", "authorization"] {
        if let Some(value) = status.metadata().get(key) {
            mapped.metadata_mut().insert(key, value.clone());
        }
    }
    mapped
}
