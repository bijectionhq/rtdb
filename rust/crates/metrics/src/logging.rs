//! The process's `tracing` subscriber, configured from the environment variables upstream's log4j2 reads.
//!
//! `XTDB_LOG_FORMAT` selects `text` (the default) or `json`.
//! `XTDB_LOGGING_LEVEL` sets the `xtdb` logger and `XTDB_LOGGING_LEVEL_<NS>` sets `xtdb.<ns>`, with `_` separating components.
//! `XTDB_LOG_JSON_TEMPLATE` names a log4j `JsonTemplateLayout` event template: one of the bundled ones or a `file:` URI.
//! RTDB's `rtdb_<crate>::<module>` targets are named `xtdb.<crate>.<module>`, with `_` inside a component written as `-`.
//! A target in one of [`UPSTREAM_ALIASES`] also answers to the upstream logger it stands for, so `XTDB_LOGGING_LEVEL_INDEXER`
//! reaches RTDB's indexer as it reaches upstream's.

use std::fmt::{self, Write as _};
use std::io::Write;
use rtdb_types::{Anomaly, Result};
use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::subscriber::Interest;
use tracing::{Event, Level, Metadata, Subscriber, span};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogFormat { Text, Json }

/// Where log lines go: upstream writes to stdout, which a command that reserves stdout for its own output cannot share.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogOutput { Stdout, Stderr }

mod template;
pub use template::{DEFAULT_TEMPLATE_URI, JsonTemplate};

/// RTDB targets that stand for one of upstream's main loggers: the target prefix, then the upstream logger name.
/// Each target keeps its own `xtdb.<crate>.<module>` name too, and the more specific configured level of the two applies.
pub const UPSTREAM_ALIASES: [(&str, &str); 7] = [
    ("rtdb_tx", "xtdb.indexer"),
    ("rtdb_index", "xtdb.indexer"),
    ("rtdb_maintenance::gc", "xtdb.garbage-collector"),
    ("rtdb_maintenance::retention", "xtdb.garbage-collector"),
    ("rtdb_maintenance", "xtdb.compactor"),
    ("rtdb_pgwire", "xtdb.pgwire"),
    ("rtdb_flight_sql", "xtdb.flight-sql"),
];

/// The upstream logger a target also answers to: the first, so most specific, alias whose prefix matches whole components.
fn upstream_alias(target: &str) -> Option<&'static str> {
    UPSTREAM_ALIASES.iter()
        .find(|(prefix, _)| target.strip_prefix(prefix).is_some_and(|rest| rest.is_empty() || rest.starts_with("::")))
        .map(|(_, alias)| *alias)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogConfig { format: LogFormat, json_template: JsonTemplate, levels: LogLevels }

#[derive(Clone, Debug, PartialEq, Eq)]
struct LogLevels {
    /// Loggers that are not RTDB's own, the counterpart of the dependency loggers upstream's `log4j2.xml` quietens.
    root: LevelFilter,
    /// Longest name first, so that the first prefix match is the most specific logger.
    loggers: Vec<(String, LevelFilter)>,
}
impl LogLevels {
    /// The configured logger that best matches `logger`, as its name's length and level.
    fn configured(&self, logger: &str) -> Option<(usize, LevelFilter)> {
        self.loggers.iter()
            .find(|(name, _)| logger.strip_prefix(name.as_str()).is_some_and(|rest| rest.is_empty() || rest.starts_with('.')))
            .map(|(name, level)| (name.len(), *level))
    }
    /// The level of a target named `logger` that may also answer to `alias`: whichever configured logger is more specific.
    fn level(&self, logger: &str, alias: Option<&str>) -> LevelFilter {
        let own = self.configured(logger);
        let aliased = alias.and_then(|alias| self.configured(alias));
        match (own, aliased) {
            (Some(own), Some(aliased)) => if aliased.0 > own.0 { aliased.1 } else { own.1 },
            (Some((_, level)), None) | (None, Some((_, level))) => level,
            (None, None) => self.root,
        }
    }
    fn set(&mut self, logger: String, level: LevelFilter) {
        self.loggers.retain(|(name, _)| *name != logger);
        self.loggers.push((logger, level));
        self.loggers.sort_by(|(left, _), (right, _)| right.len().cmp(&left.len()).then_with(|| left.cmp(right)));
    }
    fn most_verbose(&self) -> LevelFilter { self.loggers.iter().map(|(_, level)| *level).fold(self.root, LevelFilter::max) }
}

impl LogConfig {
    /// Reads every `XTDB_LOGGING_LEVEL[_NS]` variable, `XTDB_LOG_FORMAT` and `XTDB_LOG_JSON_TEMPLATE`,
    /// failing on a level or format upstream would reject and on a template RTDB cannot read or honour.
    pub fn from_environment(variables: impl IntoIterator<Item = (String, String)>) -> Result<Self> {
        let mut levels = LogLevels { root: LevelFilter::WARN, loggers: vec![("xtdb".into(), LevelFilter::INFO)] };
        let mut format = LogFormat::Text;
        let mut template_uri = None;
        for (name, value) in variables {
            if name == "XTDB_LOG_JSON_TEMPLATE" {
                // Upstream's `${env:XTDB_LOG_JSON_TEMPLATE:-…}` falls back to its default for an empty value as for an unset one.
                if !value.is_empty() { template_uri = Some(value); }
            } else if name == "XTDB_LOG_FORMAT" {
                format = match value.as_str() {
                    "text" => LogFormat::Text,
                    "json" => LogFormat::Json,
                    _ => return Err(Anomaly::incorrect("rtdb.logging/format", format!("XTDB_LOG_FORMAT must be text or json, not {value:?}"))),
                };
            } else if let Some(logger) = logger_of_variable(&name) {
                levels.set(logger, parse_level(&name, &value)?);
            }
        }
        let json_template = JsonTemplate::load(template_uri.as_deref().unwrap_or(DEFAULT_TEMPLATE_URI))?;
        Ok(Self { format, json_template, levels })
    }
    pub fn format(&self) -> LogFormat { self.format }
    pub fn json_template(&self) -> &JsonTemplate { &self.json_template }
    /// Whether an event from `target` at `level` would be written.
    pub fn enabled(&self, target: &str, level: Level) -> bool { level <= self.levels.level(&logger_name(target), upstream_alias(target)) }
}

/// The process environment, skipping variables whose name or value is not UTF-8.
pub fn process_environment() -> impl Iterator<Item = (String, String)> {
    std::env::vars_os().filter_map(|(name, value)| Some((name.into_string().ok()?, value.into_string().ok()?)))
}

/// Upstream's `xtdb_logging_level(?:_([a-z\-]+(?:_[a-z\-]+)*))?`, matched against the lower-cased variable name.
fn logger_of_variable(name: &str) -> Option<String> {
    let name = name.to_ascii_lowercase();
    let rest = name.strip_prefix("xtdb_logging_level")?;
    if rest.is_empty() { return Some("xtdb".into()); }
    let components = rest.strip_prefix('_')?.split('_').collect::<Vec<_>>();
    if components.iter().any(|component| component.is_empty() || !component.bytes().all(|byte| byte.is_ascii_lowercase() || byte == b'-')) { return None; }
    Some(std::iter::once("xtdb").chain(components).collect::<Vec<_>>().join("."))
}

/// log4j2's level names, case-insensitively; FATAL has no `tracing` counterpart and maps to ERROR, as ALL does to TRACE.
fn parse_level(variable: &str, value: &str) -> Result<LevelFilter> {
    Ok(match value.to_ascii_uppercase().as_str() {
        "OFF" => LevelFilter::OFF,
        "FATAL" | "ERROR" => LevelFilter::ERROR,
        "WARN" => LevelFilter::WARN,
        "INFO" => LevelFilter::INFO,
        "DEBUG" => LevelFilter::DEBUG,
        "TRACE" | "ALL" => LevelFilter::TRACE,
        _ => return Err(Anomaly::incorrect("rtdb.logging/level", format!("{variable} names no log level: {value:?}"))),
    })
}

/// `rtdb_storage::disk_cache` becomes `xtdb.storage.disk-cache`, and the `rtdb` binary's own target `xtdb`.
pub fn logger_name(target: &str) -> String {
    let rest = match target.strip_prefix("rtdb") {
        Some("") => return "xtdb".into(),
        Some(rest) if rest.starts_with("::") => rest,
        Some(rest) => match rest.strip_prefix('_') { Some(rest) => rest, None => return target.replace("::", ".") },
        None => return target.replace("::", "."),
    };
    let mut name = String::from("xtdb");
    for component in rest.split("::").filter(|component| !component.is_empty()) {
        name.push('.');
        name.extend(component.chars().map(|c| if c == '_' { '-' } else { c }));
    }
    name
}

enum Sink { Stdout, Stderr, #[cfg(test)] Buffer(std::sync::Arc<parking_lot::Mutex<Vec<u8>>>) }
impl Sink {
    fn write(&self, line: &[u8]) {
        let _ = match self {
            Self::Stdout => std::io::stdout().lock().write_all(line),
            Self::Stderr => std::io::stderr().lock().write_all(line),
            #[cfg(test)]
            Self::Buffer(buffer) => { buffer.lock().extend_from_slice(line); Ok(()) }
        };
    }
}

/// Writes one line per enabled event and records no spans.
struct Logger { config: LogConfig, sink: Sink, inserted: std::sync::atomic::AtomicU64 }
impl Logger {
    fn new(config: LogConfig, sink: Sink) -> Self { Self { config, sink, inserted: std::sync::atomic::AtomicU64::new(0) } }

    fn line(&self, event: &Event<'_>) -> String {
        let metadata = event.metadata();
        let mut fields = Fields::default();
        event.record(&mut fields);
        let logger = logger_name(metadata.target());
        match self.config.format {
            LogFormat::Text => {
                let mut line = format!("{} | {:<5} {logger} | {}", chrono::Local::now().format("%H:%M:%S"), metadata.level().as_str(), fields.message);
                for (name, value) in &fields.context { let _ = write!(line, " {name}={value}"); }
                line.push('\n');
                line
            }
            LogFormat::Json => {
                let thread = std::thread::current().name().unwrap_or("unnamed").to_owned();
                let template = &self.config.json_template;
                let counter = if template.uses_counter() { self.inserted.fetch_add(1, std::sync::atomic::Ordering::Relaxed) } else { 0 };
                let event = template::Event {
                    timestamp: chrono::Utc::now(), level: *metadata.level(), logger: &logger, message: &fields.message, thread: &thread,
                    context: &fields.context, file: metadata.file(), line: metadata.line(), module: metadata.module_path(), counter,
                };
                let mut line = template.render(&event).to_string();
                line.push('\n');
                line
            }
        }
    }
}
impl Subscriber for Logger {
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if self.enabled(metadata) { Interest::always() } else { Interest::never() }
    }
    fn max_level_hint(&self) -> Option<LevelFilter> { Some(self.config.levels.most_verbose()) }
    fn enabled(&self, metadata: &Metadata<'_>) -> bool { metadata.is_event() && self.config.enabled(metadata.target(), *metadata.level()) }
    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id { span::Id::from_u64(1) }
    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
    fn event(&self, event: &Event<'_>) { self.sink.write(self.line(event).as_bytes()); }
    fn enter(&self, _: &span::Id) {}
    fn exit(&self, _: &span::Id) {}
}

#[derive(Default)]
struct Fields { message: String, context: Vec<(&'static str, String)> }
impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" { self.message.push_str(value); } else { self.context.push((field.name(), value.to_owned())); }
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" { let _ = write!(self.message, "{value:?}"); } else { self.context.push((field.name(), format!("{value:?}"))); }
    }
}

/// Installs the process-wide subscriber; call it once, before the work whose events it should capture.
pub fn install(config: LogConfig, output: LogOutput) -> Result<()> {
    let sink = match output { LogOutput::Stdout => Sink::Stdout, LogOutput::Stderr => Sink::Stderr };
    tracing::subscriber::set_global_default(Logger::new(config, sink))
        .map_err(|error| Anomaly::fault("rtdb.logging/installed", format!("A log subscriber is already installed: {error}")))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use parking_lot::Mutex;
    use super::*;

    fn config(variables: &[(&str, &str)]) -> Result<LogConfig> {
        LogConfig::from_environment(variables.iter().map(|(name, value)| ((*name).to_owned(), (*value).to_owned())))
    }
    fn capture(config: LogConfig, emit: impl FnOnce()) -> String {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        tracing::subscriber::with_default(Logger::new(config, Sink::Buffer(Arc::clone(&buffer))), emit);
        let bytes = buffer.lock().clone();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn targets_are_named_like_upstream_loggers() {
        assert_eq!(logger_name("rtdb_storage::disk_cache"), "xtdb.storage.disk-cache");
        assert_eq!(logger_name("rtdb_multi"), "xtdb.multi");
        assert_eq!(logger_name("rtdb"), "xtdb");
        assert_eq!(logger_name("rtdb::follower_process"), "xtdb.follower-process");
        assert_eq!(logger_name("aws_config::profile"), "aws_config.profile");
        assert_eq!(logger_name("rtdbx::thing"), "rtdbx.thing");
    }

    #[test]
    fn upstream_environment_variables_set_levels_by_logger() {
        let defaults = config(&[]).unwrap();
        assert_eq!(defaults.format(), LogFormat::Text);
        assert!(defaults.enabled("rtdb_storage::disk_cache", Level::INFO));
        assert!(!defaults.enabled("rtdb_storage::disk_cache", Level::DEBUG));
        assert!(!defaults.enabled("hyper::client", Level::INFO));
        assert!(defaults.enabled("hyper::client", Level::WARN));

        let configured = config(&[("XTDB_LOGGING_LEVEL", "warn"), ("XTDB_LOGGING_LEVEL_STORAGE", "debug"), ("xtdb_logging_level_storage_disk-cache", "OFF"), ("XTDB_LOG_FORMAT", "json")]).unwrap();
        assert_eq!(configured.format(), LogFormat::Json);
        assert!(!configured.enabled("rtdb_node::database", Level::INFO));
        assert!(configured.enabled("rtdb_node::database", Level::WARN));
        assert!(configured.enabled("rtdb_storage::remote", Level::DEBUG));
        assert!(!configured.enabled("rtdb_storage::disk_cache", Level::ERROR));
        assert!(!configured.enabled("rtdb_storagex", Level::DEBUG), "a prefix only matches whole components");
        assert!(config(&[("XTDB_LOGGING_LEVEL_NODE_9", "debug")]).unwrap().enabled("rtdb_node", Level::INFO), "names outside upstream's pattern are ignored");
    }

    #[test]
    fn invalid_levels_and_formats_are_refused() {
        assert_eq!(config(&[("XTDB_LOGGING_LEVEL", "verbose")]).unwrap_err().code, "rtdb.logging/level");
        assert_eq!(config(&[("XTDB_LOG_FORMAT", "yaml")]).unwrap_err().code, "rtdb.logging/format");
    }

    #[test]
    fn json_templates_are_bundled_or_read_from_a_file() {
        assert_eq!(config(&[]).unwrap().json_template().uri(), DEFAULT_TEMPLATE_URI);
        assert_eq!(config(&[("XTDB_LOG_JSON_TEMPLATE", "")]).unwrap().json_template().uri(), "classpath:xtdb/logging/AwsCloudWatchLayout.json");
        assert_eq!(config(&[("XTDB_LOG_JSON_TEMPLATE", "classpath:GcpLayout.json")]).unwrap().json_template().uri(), "classpath:GcpLayout.json");
        let refused = config(&[("XTDB_LOG_JSON_TEMPLATE", "file:///nonexistent/xtdb/layout.json")]).unwrap_err();
        assert_eq!(refused.code, "rtdb.logging/json-template");

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("layout.json");
        std::fs::write(&path, r#"{"lvl": {"$resolver": "level", "field": "name"}, "msg": {"$resolver": "message"}, "app": "xtdb"}"#).unwrap();
        let uri = format!("file://{}", path.display());
        let output = capture(config(&[("XTDB_LOG_FORMAT", "json"), ("XTDB_LOG_JSON_TEMPLATE", uri.as_str())]).unwrap(), || {
            tracing::warn!(target: "rtdb_node::database", "Slow");
        });
        assert_eq!(output, "{\"lvl\":\"WARN\",\"msg\":\"Slow\",\"app\":\"xtdb\"}\n");
    }

    #[test]
    fn azure_and_gcp_lines_follow_their_templates() {
        let azure = capture(config(&[("XTDB_LOG_FORMAT", "json"), ("XTDB_LOG_JSON_TEMPLATE", "classpath:xtdb/logging/AzureMonitorLayout.json")]).unwrap(), || {
            tracing::warn!(target: "rtdb_node::database", trace_id = "abc", database = "xtdb", "Slow");
        });
        let line: serde_json::Value = serde_json::from_str(azure.trim_end()).unwrap();
        assert_eq!(line["Severity"], "WARN");
        assert_eq!(line["Message"], "Slow");
        assert_eq!(line["OperationId"], "abc");
        assert_eq!(line["Attributes"]["log.logger.name"], "xtdb.node.database");
        assert_eq!(line["Attributes"]["custom.context"]["database"], "xtdb");
        assert!(line["Timestamp"].as_str().unwrap().ends_with('Z'));
        assert!(line.get("Exception").is_none());

        let gcp = capture(config(&[("XTDB_LOG_FORMAT", "json"), ("XTDB_LOG_JSON_TEMPLATE", "classpath:GcpLayout.json")]).unwrap(), || {
            tracing::warn!(target: "rtdb_node::database", database = "xtdb", "Slow");
            tracing::error!(target: "rtdb_node::database", "Failed");
        });
        let lines = gcp.lines().map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap()).collect::<Vec<_>>();
        assert_eq!(lines[0]["severity"], "WARNING");
        assert_eq!(lines[1]["severity"], "ERROR");
        assert_eq!(lines[0]["message"], "Slow");
        assert_eq!(lines[0]["logger"], "xtdb.node.database");
        assert_eq!(lines[0]["logging.googleapis.com/labels"]["database"], "xtdb");
        assert_eq!(lines[0]["logging.googleapis.com/trace_sampled"], true);
        assert!(lines[0]["timestampSeconds"].is_i64());
        assert!(lines[0]["timestampNanos"].is_i64());
    }

    #[test]
    fn upstream_logger_names_reach_the_rtdb_targets_that_stand_for_them() {
        let configured = config(&[("XTDB_LOGGING_LEVEL_INDEXER", "debug"), ("XTDB_LOGGING_LEVEL_GARBAGE-COLLECTOR", "error"), ("XTDB_LOGGING_LEVEL_COMPACTOR", "warn")]).unwrap();
        assert!(configured.enabled("rtdb_tx::transact", Level::DEBUG));
        assert!(configured.enabled("rtdb_index::live", Level::DEBUG));
        assert!(!configured.enabled("rtdb_maintenance::gc", Level::WARN));
        assert!(configured.enabled("rtdb_maintenance::gc", Level::ERROR));
        assert!(!configured.enabled("rtdb_maintenance::merge", Level::INFO));
        assert!(configured.enabled("rtdb_maintenance::merge", Level::WARN));
        assert!(!configured.enabled("rtdb_txx", Level::DEBUG), "an alias prefix only matches whole components");

        let own_name_wins_when_more_specific = config(&[("XTDB_LOGGING_LEVEL_INDEXER", "debug"), ("XTDB_LOGGING_LEVEL_TX_TRANSACT", "error")]).unwrap();
        assert!(!own_name_wins_when_more_specific.enabled("rtdb_tx::transact", Level::WARN));
        assert!(own_name_wins_when_more_specific.enabled("rtdb_tx::resolver", Level::DEBUG));
    }

    #[test]
    fn text_lines_follow_upstream_pattern_and_respect_levels() {
        let output = capture(config(&[]).unwrap(), || {
            tracing::warn!(target: "rtdb_storage::disk_cache", path = "/tmp/x", "Disk cache eviction failed");
            tracing::debug!(target: "rtdb_storage::disk_cache", "not written");
            tracing::info!(target: "hyper::client", "not written either");
        });
        let lines = output.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 1, "{output}");
        let (time, rest) = lines[0].split_once(" | ").unwrap();
        assert_eq!(time.len(), 8);
        assert_eq!(rest, "WARN  xtdb.storage.disk-cache | Disk cache eviction failed path=/tmp/x");
    }

    #[test]
    fn json_lines_carry_upstream_layout_fields() {
        let output = capture(config(&[("XTDB_LOG_FORMAT", "json")]).unwrap(), || {
            tracing::error!(target: "rtdb_node::database", database = "xtdb", "Custody reconciliation failed");
        });
        let line: serde_json::Value = serde_json::from_str(output.trim_end()).unwrap();
        assert_eq!(line["level"], "ERROR");
        assert_eq!(line["logger"], "xtdb.node.database");
        assert_eq!(line["message"], "Custody reconciliation failed");
        assert_eq!(line["context"]["database"], "xtdb");
        assert!(line["timestamp"].as_str().unwrap().ends_with('Z'));
        assert!(line["thread"].is_string());
    }
}
