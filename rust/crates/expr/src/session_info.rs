use crate::EvalContext;
use rtdb_types::{Anomaly, Result, Value, runtime_revision, runtime_version};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SessionInfoFunction {
    CurrentUser,
    CurrentDatabase,
    CurrentSchema,
    CurrentSchemas,
    CurrentSetting,
    Version,
    XtVersion,
}

impl SessionInfoFunction {
    pub(crate) fn from_canonical(name: &str) -> Option<Self> {
        Some(match name {
            "current_user" => Self::CurrentUser,
            "current_database" => Self::CurrentDatabase,
            "current_schema" => Self::CurrentSchema,
            "current_schemas" => Self::CurrentSchemas,
            "current_setting" => Self::CurrentSetting,
            "version" => Self::Version,
            "xt.version" => Self::XtVersion,
            _ => return None,
        })
    }

    pub(crate) fn canonical_name(self) -> &'static str {
        match self {
            Self::CurrentUser => "current_user",
            Self::CurrentDatabase => "current_database",
            Self::CurrentSchema => "current_schema",
            Self::CurrentSchemas => "current_schemas",
            Self::CurrentSetting => "current_setting",
            Self::Version => "version",
            Self::XtVersion => "xt.version",
        }
    }

    pub(crate) fn eval(self, arguments: &[Value], context: &EvalContext<'_>) -> Result<Value> {
        match self {
            Self::CurrentUser | Self::CurrentDatabase => text("xtdb", context),
            Self::CurrentSchema => text("public", context),
            Self::CurrentSchemas => match &arguments[0] {
                Value::Null | Value::Undefined => Ok(Value::Null),
                Value::Bool(implicit) => {
                    let names: &[&str] = if *implicit { &["pg_catalog", "public"] } else { &["public"] };
                    context.reserve(names.len() * std::mem::size_of::<Value>() + names.iter().map(|name| name.len()).sum::<usize>()).map(|()|
                        Value::list(names.iter().map(|name| Value::Utf8((*name).into())).collect()))
                }
                _ => Err(Anomaly::incorrect("xtdb.expression/function-type-mismatch", "current_schemas requires a boolean")),
            },
            Self::CurrentSetting => match &arguments[0] {
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
            Self::Version => context.reserve(runtime_version().len().saturating_add(32).saturating_mul(2)).map(|()| Value::Utf8(format!("PostgreSQL 16 (XTDB {})", runtime_version()))),
            Self::XtVersion => context.reserve(runtime_version().len().saturating_add(32).saturating_mul(2)).map(|()| Value::Utf8(match runtime_revision() {
                Some(revision) => format!("XTDB @ {} [{revision}]", runtime_version()),
                None => format!("XTDB @ {}", runtime_version()),
            })),
        }
    }
}

fn text(value: &str, context: &EvalContext<'_>) -> Result<Value> {
    context.reserve(value.len())?;
    Ok(Value::Utf8(value.into()))
}
