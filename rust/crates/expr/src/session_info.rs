use crate::EvalContext;
use rtdb_types::{Anomaly, Result, Value, runtime_revision, runtime_version};

pub(crate) fn call(name: &str, arguments: &[Value], context: &EvalContext<'_>) -> Option<Result<Value>> {
    Some(match name {
        "current_user" | "current_database" => text("xtdb", context),
        "current_schema" => text("public", context),
        "current_schemas" => match &arguments[0] {
            Value::Null | Value::Undefined => Ok(Value::Null),
            Value::Bool(implicit) => {
                let names: &[&str] = if *implicit { &["pg_catalog", "public"] } else { &["public"] };
                context.reserve(names.len() * std::mem::size_of::<Value>() + names.iter().map(|name| name.len()).sum::<usize>()).map(|()|
                    Value::list(names.iter().map(|name| Value::Utf8((*name).into())).collect()))
            }
            _ => Err(Anomaly::incorrect("xtdb.expression/function-type-mismatch", "current_schemas requires a boolean")),
        },
        "current_setting" => match &arguments[0] {
            Value::Null | Value::Undefined => Ok(Value::Null),
            Value::Utf8(setting) => match setting.as_str() {
                "server_version_num" => text("160000", context),
                "search_path" => text("public", context),
                "xtdb.version" => text(runtime_version(), context),
                _ => Err(Anomaly::unsupported("xtdb.expression/unsupported-setting", format!("Setting not supported: {setting}"))
                    .with_context("setting-name", serde_json::Value::String(setting.clone()))),
            },
            _ => Err(Anomaly::incorrect("xtdb.expression/function-type-mismatch", "current_setting requires a string")),
        },
        "version" => context.reserve(runtime_version().len().saturating_add(32).saturating_mul(2)).map(|()| Value::Utf8(format!("PostgreSQL 16 (XTDB {})", runtime_version()))),
        "xt.version" => context.reserve(runtime_version().len().saturating_add(32).saturating_mul(2)).map(|()| Value::Utf8(match runtime_revision() {
            Some(revision) => format!("XTDB @ {} [{revision}]", runtime_version()),
            None => format!("XTDB @ {}", runtime_version()),
        })),
        _ => return None,
    })
}

fn text(value: &str, context: &EvalContext<'_>) -> Result<Value> {
    context.reserve(value.len())?;
    Ok(Value::Utf8(value.into()))
}
