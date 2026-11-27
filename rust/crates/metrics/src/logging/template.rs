//! log4j's `JsonTemplateLayout` event templates, which upstream selects with `XTDB_LOG_JSON_TEMPLATE`.
//!
//! A template is read and compiled once, at startup, from a `classpath:` URI naming one of the bundled templates
//! (upstream's `xtdb/logging/AwsCloudWatchLayout.json` and `AzureMonitorLayout.json`, and log4j 2.26.1's own
//! `GcpLayout.json`, Apache-2.0) or from a `file:` URI. A resolver or option RTDB cannot honour refuses startup rather
//! than writing a different line. `tracing` events carry no exception or marker, so those resolvers resolve to nothing.

use chrono::{DateTime, Datelike, FixedOffset, Offset, TimeZone, Timelike, Utc};
use rtdb_types::{Anomaly, Result};
use serde_json::{Map, Value};
use tracing::Level;

const BUNDLED: [(&str, &str); 3] = [
    ("classpath:xtdb/logging/AwsCloudWatchLayout.json", include_str!("templates/AwsCloudWatchLayout.json")),
    ("classpath:xtdb/logging/AzureMonitorLayout.json", include_str!("templates/AzureMonitorLayout.json")),
    ("classpath:GcpLayout.json", include_str!("templates/GcpLayout.json")),
];

/// Upstream's default, `log4j2.xml`'s `${env:XTDB_LOG_JSON_TEMPLATE:-classpath:xtdb/logging/AwsCloudWatchLayout.json}`.
pub const DEFAULT_TEMPLATE_URI: &str = BUNDLED[0].0;

const MAX_TEMPLATE_BYTES: u64 = 1 << 20;

fn refused(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("rtdb.logging/json-template", message) }

/// One event, as the resolvers see it.
pub(super) struct Event<'a> {
    pub timestamp: DateTime<Utc>,
    pub level: Level,
    pub logger: &'a str,
    pub message: &'a str,
    pub thread: &'a str,
    pub context: &'a [(&'static str, String)],
    pub file: Option<&'a str>,
    pub line: Option<u32>,
    pub module: Option<&'a str>,
    pub counter: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonTemplate { uri: String, root: Node }

#[derive(Clone, Debug, PartialEq, Eq)]
enum Node {
    Constant(Value),
    Object(Vec<(String, Node)>),
    Array(Vec<Node>),
    Resolver(Resolver),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Resolver {
    Timestamp(Timestamp),
    Level(LevelField),
    Logger,
    Message,
    Thread,
    Mdc { key: Option<String>, flatten: Option<String> },
    Source(SourceField),
    Counter { start: i64, stringified: bool },
    Pattern(Vec<Part>),
    /// A resolver over something `tracing` events never carry: an exception, a marker.
    Absent,
    Constant(Value),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LevelField { Name, SeverityKeyword, SeverityCode }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SourceField { File, Line, Class }

#[derive(Clone, Debug, PartialEq, Eq)]
enum Timestamp {
    Pattern { format: Vec<DatePart>, zone: Zone },
    Epoch { unit: EpochUnit, rounded: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Zone { Local, Fixed(i32) }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EpochUnit { Nanos, Millis, Secs, MillisNanos, SecsNanos }

#[derive(Clone, Debug, PartialEq, Eq)]
enum DatePart { Literal(String), Field(char, usize) }

#[derive(Clone, Debug, PartialEq, Eq)]
enum Part {
    Literal(String),
    Message,
    Newline,
    Level(Vec<(String, String)>),
    Logger,
    Thread,
    Class,
    Method,
    File,
    Line,
    /// `%replace{pattern}{^literal$}{replacement}`: only a whole-value literal match, which is what log4j's GcpLayout uses.
    Replace { inner: Vec<Part>, whole: String, replacement: String },
}

impl JsonTemplate {
    /// Reads the template `uri` names, as log4j reads `eventTemplateUri`.
    /// - Errors: a URI that is neither a bundled `classpath:` template nor a readable `file:`, malformed JSON, or a resolver RTDB cannot honour.
    pub fn load(uri: &str) -> Result<Self> {
        let text = if let Some((_, text)) = BUNDLED.iter().find(|(bundled, _)| *bundled == uri) {
            (*text).to_owned()
        } else if uri.starts_with("classpath:") {
            let known = BUNDLED.iter().map(|(uri, _)| *uri).collect::<Vec<_>>().join(", ");
            return Err(refused(format!("XTDB_LOG_JSON_TEMPLATE names {uri:?}; the bundled templates are {known}, and any other template is read from a file: URI")));
        } else if let Some(path) = uri.strip_prefix("file:") {
            read_file(&file_path(path).ok_or_else(|| refused(format!("XTDB_LOG_JSON_TEMPLATE names {uri:?}, which is not an absolute file: URI")))?)?
        } else {
            return Err(refused(format!("XTDB_LOG_JSON_TEMPLATE names {uri:?}, which is neither a classpath: nor a file: URI")));
        };
        Self::parse(uri, &text)
    }

    fn parse(uri: &str, text: &str) -> Result<Self> {
        let value: Value = serde_json::from_str(text).map_err(|error| refused(format!("The JSON template {uri} is not JSON: {error}")))?;
        if !value.is_object() { return Err(refused(format!("The JSON template {uri} is not a JSON object"))); }
        Ok(Self { uri: uri.to_owned(), root: compile(&value).map_err(|error| refused(format!("The JSON template {uri}: {}", error.message.unwrap_or_default())))? })
    }

    pub fn uri(&self) -> &str { &self.uri }

    pub(super) fn uses_counter(&self) -> bool { self.root.uses_counter() }

    pub(super) fn render(&self, event: &Event<'_>) -> Value {
        self.root.resolve(event).unwrap_or_else(|| Value::Object(Map::new()))
    }
}

fn read_file(path: &std::path::Path) -> Result<String> {
    use std::io::Read;
    let mut text = String::new();
    std::fs::File::open(path)
        .and_then(|file| file.take(MAX_TEMPLATE_BYTES + 1).read_to_string(&mut text))
        .map_err(|error| refused(format!("The JSON template {} cannot be read: {error}", path.display())))?;
    if text.len() as u64 > MAX_TEMPLATE_BYTES { return Err(refused(format!("The JSON template {} is over 1 MiB", path.display()))); }
    Ok(text)
}

/// `file:///path`, `file://localhost/path` or `file:/path`, percent-decoded.
fn file_path(rest: &str) -> Option<std::path::PathBuf> {
    let path = match rest.strip_prefix("//") {
        Some(authority) => { let slash = authority.find('/')?; if !matches!(&authority[..slash], "" | "localhost") { return None; } &authority[slash..] }
        None => rest,
    };
    if !path.starts_with('/') { return None; }
    let bytes = path.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hex = std::str::from_utf8(bytes.get(index + 1..index + 3)?).ok()?;
            decoded.push(u8::from_str_radix(hex, 16).ok()?);
            index += 3;
        } else { decoded.push(bytes[index]); index += 1; }
    }
    String::from_utf8(decoded).ok().map(std::path::PathBuf::from)
}

fn invalid(message: impl Into<String>) -> Anomaly { refused(message) }

fn compile(value: &Value) -> Result<Node> {
    Ok(match value {
        Value::Object(fields) if fields.contains_key("$resolver") => Node::Resolver(resolver(fields)?),
        Value::Object(fields) => Node::Object(fields.iter().map(|(key, value)| Ok((key.clone(), compile(value)?))).collect::<Result<_>>()?),
        Value::Array(values) => Node::Array(values.iter().map(compile).collect::<Result<_>>()?),
        constant => Node::Constant(constant.clone()),
    })
}

fn string<'a>(fields: &'a Map<String, Value>, key: &str) -> Result<Option<&'a str>> {
    match fields.get(key) {
        None => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(invalid(format!("{key} must be a string"))),
    }
}

fn boolean(fields: &Map<String, Value>, key: &str) -> Result<bool> {
    match fields.get(key) {
        None => Ok(false),
        Some(Value::Bool(value)) => Ok(*value),
        Some(_) => Err(invalid(format!("{key} must be true or false"))),
    }
}

fn resolver(fields: &Map<String, Value>) -> Result<Resolver> {
    let name = string(fields, "$resolver")?.unwrap_or_default();
    let field = string(fields, "field")?;
    let unsupported = |what: &str| invalid(format!("the {name} resolver's {what} is not supported"));
    Ok(match name {
        "timestamp" => Resolver::Timestamp(timestamp(fields)?),
        "level" => Resolver::Level(match field {
            Some("name") => LevelField::Name,
            Some("severity") => {
                let severity = fields.get("severity").and_then(Value::as_object);
                match severity.map(|severity| string(severity, "field")).transpose()?.flatten() {
                    Some("keyword") => LevelField::SeverityKeyword,
                    Some("code") => LevelField::SeverityCode,
                    _ => return Err(unsupported("severity field")),
                }
            }
            _ => return Err(unsupported("field")),
        }),
        "logger" => match field { Some("name") => Resolver::Logger, _ => return Err(unsupported("field")) },
        "thread" => match field { Some("name") => Resolver::Thread, _ => return Err(unsupported("field")) },
        "message" => {
            if fields.contains_key("pattern") { return Err(unsupported("pattern")); }
            Resolver::Message
        }
        "mdc" => {
            if fields.contains_key("pattern") || fields.contains_key("replacement") { return Err(unsupported("pattern")); }
            let flatten = match fields.get("flatten") {
                None | Some(Value::Bool(false)) => None,
                Some(Value::Bool(true)) => Some(String::new()),
                Some(Value::Object(flatten)) => Some(string(flatten, "prefix")?.unwrap_or_default().to_owned()),
                Some(_) => return Err(unsupported("flatten")),
            };
            Resolver::Mdc { key: string(fields, "key")?.map(str::to_owned), flatten }
        }
        "source" => Resolver::Source(match field {
            Some("fileName") => SourceField::File,
            Some("lineNumber") => SourceField::Line,
            Some("className") => SourceField::Class,
            Some("methodName") => return Ok(Resolver::Absent),
            _ => return Err(unsupported("field")),
        }),
        "counter" => Resolver::Counter {
            start: match fields.get("start") { None => 0, Some(value) => value.as_i64().ok_or_else(|| unsupported("start"))? },
            stringified: boolean(fields, "stringified")?,
        },
        "pattern" => Resolver::Pattern(pattern(string(fields, "pattern")?.ok_or_else(|| invalid("the pattern resolver needs a pattern"))?)?),
        "exception" | "exceptionRootCause" | "marker" => Resolver::Absent,
        "endOfBatch" => Resolver::Constant(Value::Bool(false)),
        other => return Err(invalid(format!("the {other:?} resolver is not supported"))),
    })
}

fn timestamp(fields: &Map<String, Value>) -> Result<Timestamp> {
    if let Some(epoch) = fields.get("epoch") {
        let epoch = epoch.as_object().ok_or_else(|| invalid("the timestamp epoch must be an object"))?;
        let unit = match string(epoch, "unit")?.unwrap_or("secs") {
            "nanos" => EpochUnit::Nanos, "millis" => EpochUnit::Millis, "secs" => EpochUnit::Secs,
            "millis.nanos" => EpochUnit::MillisNanos, "secs.nanos" => EpochUnit::SecsNanos,
            other => return Err(invalid(format!("the timestamp epoch unit {other:?} is not supported"))),
        };
        return Ok(Timestamp::Epoch { unit, rounded: boolean(epoch, "rounded")? });
    }
    let pattern = match fields.get("pattern") { None => Map::new(), Some(Value::Object(pattern)) => pattern.clone(), Some(_) => return Err(invalid("the timestamp pattern must be an object")) };
    let format = date_format(string(&pattern, "format")?.unwrap_or("yyyy-MM-dd'T'HH:mm:ss.SSSZZZ"))?;
    let zone = match string(&pattern, "timeZone")? {
        None => Zone::Local,
        Some("UTC" | "GMT" | "Z" | "Etc/UTC" | "Etc/GMT" | "UT") => Zone::Fixed(0),
        Some(offset) => Zone::Fixed(fixed_offset(offset).ok_or_else(|| invalid(format!("the time zone {offset:?} is not supported; use UTC or a fixed offset such as +01:00")))?),
    };
    Ok(Timestamp::Pattern { format, zone })
}

/// `+01:00`, `-0530`, `+01`, optionally after `UTC` or `GMT`, in seconds east of UTC.
fn fixed_offset(text: &str) -> Option<i32> {
    let text = text.strip_prefix("UTC").or_else(|| text.strip_prefix("GMT")).unwrap_or(text);
    let (sign, digits) = match text.as_bytes().first()? { b'+' => (1, &text[1..]), b'-' => (-1, &text[1..]), _ => return None };
    let digits = digits.replace(':', "");
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) { return None; }
    let (hours, minutes) = match digits.len() { 1 | 2 => (digits.parse::<i32>().ok()?, 0), 4 => (digits[..2].parse().ok()?, digits[2..].parse().ok()?), _ => return None };
    (hours <= 18 && minutes < 60).then_some(sign * (hours * 3600 + minutes * 60))
}

/// A `DateTimeFormatter` pattern, over the letters a log line's timestamp uses.
fn date_format(format: &str) -> Result<Vec<DatePart>> {
    let mut parts = Vec::new();
    let mut literal = String::new();
    let mut characters = format.chars().peekable();
    while let Some(character) = characters.next() {
        if character == '\'' {
            if characters.peek() == Some(&'\'') { characters.next(); literal.push('\''); continue; }
            loop {
                match characters.next() {
                    None => return Err(invalid(format!("the timestamp format {format:?} has an unterminated quote"))),
                    Some('\'') if characters.peek() == Some(&'\'') => { characters.next(); literal.push('\''); }
                    Some('\'') => break,
                    Some(quoted) => literal.push(quoted),
                }
            }
        } else if character.is_ascii_alphabetic() {
            let mut count = 1;
            while characters.peek() == Some(&character) { characters.next(); count += 1; }
            let supported = match character {
                'y' | 'u' | 'd' | 'H' | 'h' | 'm' | 's' | 'a' => count <= 4,
                'M' | 'E' => count <= 4,
                'S' => count <= 9,
                'Z' => count <= 3,
                'X' | 'x' => count <= 3,
                _ => false,
            };
            if !supported { return Err(invalid(format!("the timestamp format {format:?} uses {:?}, which is not supported", character.to_string().repeat(count)))); }
            if !literal.is_empty() { parts.push(DatePart::Literal(std::mem::take(&mut literal))); }
            parts.push(DatePart::Field(character, count));
        } else {
            literal.push(character);
        }
    }
    if !literal.is_empty() { parts.push(DatePart::Literal(literal)); }
    Ok(parts)
}

fn format_date(parts: &[DatePart], time: &DateTime<FixedOffset>) -> String {
    const MONTHS: [&str; 12] = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
    const DAYS: [&str; 7] = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
    let mut out = String::new();
    let padded = |out: &mut String, value: i64, width: usize| out.push_str(&format!("{value:0width$}"));
    for part in parts {
        match part {
            DatePart::Literal(text) => out.push_str(text),
            DatePart::Field(letter, count) => {
                let count = *count;
                match letter {
                    'y' | 'u' if count == 2 => padded(&mut out, i64::from(time.year()).rem_euclid(100), 2),
                    'y' | 'u' => padded(&mut out, i64::from(time.year()), count),
                    'M' if count == 3 => out.push_str(&MONTHS[time.month0() as usize][..3]),
                    'M' if count == 4 => out.push_str(MONTHS[time.month0() as usize]),
                    'M' => padded(&mut out, i64::from(time.month()), count),
                    'E' if count == 4 => out.push_str(DAYS[time.weekday().num_days_from_monday() as usize]),
                    'E' => out.push_str(&DAYS[time.weekday().num_days_from_monday() as usize][..3]),
                    'd' => padded(&mut out, i64::from(time.day()), count),
                    'H' => padded(&mut out, i64::from(time.hour()), count),
                    'h' => padded(&mut out, i64::from(time.hour12().1), count),
                    'a' => out.push_str(if time.hour12().0 { "PM" } else { "AM" }),
                    'm' => padded(&mut out, i64::from(time.minute()), count),
                    's' => padded(&mut out, i64::from(time.second()), count),
                    'S' => out.push_str(&format!("{:09}", time.nanosecond().min(999_999_999))[..count]),
                    'Z' | 'X' | 'x' => {
                        let offset = time.offset().fix().local_minus_utc();
                        if *letter == 'X' && offset == 0 { out.push('Z'); continue; }
                        let sign = if offset < 0 { '-' } else { '+' };
                        let (hours, minutes) = (offset.abs() / 3600, offset.abs() % 3600 / 60);
                        match (letter, count) {
                            ('X' | 'x', 1) if minutes == 0 => out.push_str(&format!("{sign}{hours:02}")),
                            ('X' | 'x', 3) => out.push_str(&format!("{sign}{hours:02}:{minutes:02}")),
                            _ => out.push_str(&format!("{sign}{hours:02}{minutes:02}")),
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    out
}

/// A `PatternLayout` pattern, over the converters a JSON template's pattern resolver uses.
fn pattern(text: &str) -> Result<Vec<Part>> {
    let mut parser = PatternParser { text, position: 0 };
    let parts = parser.parts(false)?;
    Ok(parts)
}

struct PatternParser<'a> { text: &'a str, position: usize }
impl PatternParser<'_> {
    fn rest(&self) -> &str { &self.text[self.position..] }
    fn parts(&mut self, nested: bool) -> Result<Vec<Part>> {
        let mut parts = Vec::new();
        let mut literal = String::new();
        while let Some(character) = self.rest().chars().next() {
            if nested && character == '}' { break; }
            self.position += character.len_utf8();
            if character != '%' { literal.push(character); continue; }
            if self.rest().starts_with('%') { self.position += 1; literal.push('%'); continue; }
            let name: String = self.rest().chars().take_while(char::is_ascii_alphabetic).collect();
            if name.is_empty() { return Err(invalid(format!("the pattern {:?} has a converter RTDB does not support", self.text))); }
            self.position += name.len();
            if !literal.is_empty() { parts.push(Part::Literal(std::mem::take(&mut literal))); }
            let part = match name.as_str() {
                "m" | "msg" | "message" => Part::Message,
                "n" => Part::Newline,
                "p" | "level" | "le" => {
                    let mut mapping = Vec::new();
                    if let Some(options) = self.option()? {
                        for entry in options.split(',') {
                            let (from, to) = entry.split_once('=').ok_or_else(|| invalid(format!("the level option {options:?} is not supported")))?;
                            mapping.push((from.trim().to_ascii_uppercase(), to.trim().to_owned()));
                        }
                    }
                    Part::Level(mapping)
                }
                "c" | "logger" => { self.option()?; Part::Logger }
                "t" | "thread" | "tn" | "threadName" => Part::Thread,
                "C" | "class" => { self.option()?; Part::Class }
                "M" | "method" => Part::Method,
                "F" | "file" => Part::File,
                "L" | "line" => Part::Line,
                "replace" => {
                    self.expect_open()?;
                    let inner = self.parts(true)?;
                    self.expect_close()?;
                    let regex = self.option()?.ok_or_else(|| invalid("%replace needs a pattern"))?;
                    let replacement = self.option()?.ok_or_else(|| invalid("%replace needs a replacement"))?;
                    let whole = whole_literal(&regex).ok_or_else(|| invalid(format!("%replace supports only a whole literal match such as ^\\?\\.$, not {regex:?}")))?;
                    Part::Replace { inner, whole, replacement }
                }
                other => return Err(invalid(format!("the pattern converter %{other} is not supported"))),
            };
            parts.push(part);
        }
        if !literal.is_empty() { parts.push(Part::Literal(literal)); }
        Ok(parts)
    }
    fn expect_open(&mut self) -> Result<()> {
        if !self.rest().starts_with('{') { return Err(invalid(format!("the pattern {:?} is missing a {{", self.text))); }
        self.position += 1;
        Ok(())
    }
    fn expect_close(&mut self) -> Result<()> {
        if !self.rest().starts_with('}') { return Err(invalid(format!("the pattern {:?} is missing a }}", self.text))); }
        self.position += 1;
        Ok(())
    }
    fn option(&mut self) -> Result<Option<String>> {
        if !self.rest().starts_with('{') { return Ok(None); }
        let end = self.rest().find('}').ok_or_else(|| invalid(format!("the pattern {:?} is missing a }}", self.text)))?;
        let option = self.rest()[1..end].to_owned();
        self.position += end + 1;
        Ok(Some(option))
    }
}

/// The literal a `^…$` regular expression matches exactly, with `\` escapes; `None` for anything else.
fn whole_literal(regex: &str) -> Option<String> {
    let body = regex.strip_prefix('^')?.strip_suffix('$')?;
    let mut literal = String::new();
    let mut characters = body.chars();
    while let Some(character) = characters.next() {
        match character {
            '\\' => { let escaped = characters.next()?; if escaped.is_ascii_alphanumeric() { return None; } literal.push(escaped); }
            '.' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '^' | '$' => return None,
            other => literal.push(other),
        }
    }
    Some(literal)
}

fn render_parts(parts: &[Part], event: &Event<'_>, out: &mut String) {
    for part in parts {
        match part {
            Part::Literal(text) => out.push_str(text),
            Part::Message => out.push_str(event.message),
            Part::Newline => out.push('\n'),
            Part::Level(mapping) => {
                let name = event.level.as_str();
                out.push_str(mapping.iter().find(|(from, _)| from == name).map_or(name, |(_, to)| to.as_str()));
            }
            Part::Logger => out.push_str(event.logger),
            Part::Thread => out.push_str(event.thread),
            Part::Class => out.push_str(event.module.unwrap_or("?")),
            // `tracing` records no function name, which log4j writes as empty, as for an unknown location.
            Part::Method => {}
            Part::File => out.push_str(event.file.unwrap_or("?")),
            Part::Line => out.push_str(&event.line.map_or_else(|| "?".to_owned(), |line| line.to_string())),
            Part::Replace { inner, whole, replacement } => {
                let mut value = String::new();
                render_parts(inner, event, &mut value);
                out.push_str(if value == *whole { replacement } else { &value });
            }
        }
    }
}

impl Node {
    fn uses_counter(&self) -> bool {
        match self {
            Self::Resolver(Resolver::Counter { .. }) => true,
            Self::Object(fields) => fields.iter().any(|(_, node)| node.uses_counter()),
            Self::Array(nodes) => nodes.iter().any(Node::uses_counter),
            _ => false,
        }
    }

    /// The node's value, or `None` when it resolves to nothing and its field is left out, as log4j leaves out a field whose resolver is not resolvable.
    fn resolve(&self, event: &Event<'_>) -> Option<Value> {
        match self {
            Self::Constant(value) => Some(value.clone()),
            Self::Array(nodes) => Some(Value::Array(nodes.iter().map(|node| node.resolve(event).unwrap_or(Value::Null)).collect())),
            Self::Object(fields) => {
                let mut object = Map::new();
                for (key, node) in fields {
                    match node {
                        Self::Resolver(Resolver::Mdc { key: None, flatten: Some(prefix) }) => {
                            for (name, value) in event.context { object.insert(format!("{prefix}{name}"), Value::from(value.clone())); }
                        }
                        node => if let Some(value) = node.resolve(event) { object.insert(key.clone(), value); },
                    }
                }
                (!object.is_empty()).then_some(Value::Object(object))
            }
            Self::Resolver(resolver) => resolver.resolve(event),
        }
    }
}

impl Resolver {
    fn resolve(&self, event: &Event<'_>) -> Option<Value> {
        Some(match self {
            Self::Timestamp(Timestamp::Pattern { format, zone }) => {
                let offset = match zone {
                    Zone::Fixed(seconds) => FixedOffset::east_opt(*seconds)?,
                    Zone::Local => chrono::Local.offset_from_utc_datetime(&event.timestamp.naive_utc()).fix(),
                };
                Value::from(format_date(format, &event.timestamp.with_timezone(&offset)))
            }
            Self::Timestamp(Timestamp::Epoch { unit, rounded }) => {
                let nanos = event.timestamp.timestamp_nanos_opt()?;
                match (unit, rounded) {
                    (EpochUnit::Nanos, _) => Value::from(nanos),
                    (EpochUnit::Millis, true) => Value::from(nanos.div_euclid(1_000_000)),
                    (EpochUnit::Millis, false) => Value::from(nanos as f64 / 1e6),
                    (EpochUnit::Secs, true) => Value::from(nanos.div_euclid(1_000_000_000)),
                    (EpochUnit::Secs, false) => Value::from(nanos as f64 / 1e9),
                    (EpochUnit::MillisNanos, _) => Value::from(nanos.rem_euclid(1_000_000)),
                    (EpochUnit::SecsNanos, _) => Value::from(nanos.rem_euclid(1_000_000_000)),
                }
            }
            Self::Level(field) => {
                // log4j's `Severity.getSeverity`: syslog severities.
                let (keyword, code) = match event.level {
                    Level::ERROR => ("ERROR", 3), Level::WARN => ("WARNING", 4), Level::INFO => ("INFO", 6), _ => ("DEBUG", 7),
                };
                match field { LevelField::Name => Value::from(event.level.as_str()), LevelField::SeverityKeyword => Value::from(keyword), LevelField::SeverityCode => Value::from(code) }
            }
            Self::Logger => Value::from(event.logger),
            Self::Message => Value::from(event.message),
            Self::Thread => Value::from(event.thread),
            Self::Mdc { key: Some(key), .. } => Value::from(event.context.iter().find(|(name, _)| name == key)?.1.clone()),
            Self::Mdc { key: None, .. } => {
                if event.context.is_empty() { return None; }
                Value::Object(event.context.iter().map(|(name, value)| ((*name).to_owned(), Value::from(value.clone()))).collect())
            }
            Self::Source(SourceField::File) => Value::from(event.file?),
            Self::Source(SourceField::Line) => Value::from(event.line?),
            Self::Source(SourceField::Class) => Value::from(event.module?),
            Self::Counter { start, stringified } => {
                let value = i128::from(*start) + i128::from(event.counter);
                if *stringified { Value::from(value.to_string()) } else { Value::from(i64::try_from(value).ok()?) }
            }
            Self::Pattern(parts) => { let mut out = String::new(); render_parts(parts, event, &mut out); Value::from(out) }
            Self::Absent => return None,
            Self::Constant(value) => value.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event<'a>(level: Level, context: &'a [(&'static str, String)]) -> Event<'a> {
        Event {
            timestamp: Utc.with_ymd_and_hms(2026, 11, 27, 9, 5, 7).unwrap() + chrono::Duration::nanoseconds(123_456_789),
            level, logger: "xtdb.node.database", message: "Slow", thread: "main", context,
            file: Some("node/src/database.rs"), line: Some(42), module: Some("rtdb_node::database"), counter: 3,
        }
    }

    #[test]
    fn every_bundled_template_compiles() {
        for (uri, _) in BUNDLED { JsonTemplate::load(uri).unwrap(); }
    }

    #[test]
    fn the_gcp_template_is_log4js() {
        let template = JsonTemplate::load("classpath:GcpLayout.json").unwrap();
        let context = [("trace_id", "abc".to_owned()), ("database", "xtdb".to_owned())];
        let line = template.render(&event(Level::WARN, &context));
        assert_eq!(line["timestampSeconds"], 1_795_770_307i64);
        assert_eq!(line["timestampNanos"], 123_456_789);
        assert_eq!(line["severity"], "WARNING");
        assert_eq!(line["message"], "Slow");
        assert_eq!(line["logging.googleapis.com/labels"]["database"], "xtdb");
        assert_eq!(line["logging.googleapis.com/trace"], "abc");
        assert!(line.get("logging.googleapis.com/spanId").is_none());
        assert_eq!(line["logging.googleapis.com/trace_sampled"], true);
        assert_eq!(line["logging.googleapis.com/sourceLocation"]["file"], "node/src/database.rs");
        assert_eq!(line["logging.googleapis.com/sourceLocation"]["line"], 42);
        assert_eq!(line["thread"], "main");
        assert_eq!(line["logger"], "xtdb.node.database");
        assert!(line.get("exception").is_none());
        assert_eq!(template.render(&event(Level::TRACE, &[]))["severity"], "DEBUG");
    }

    #[test]
    fn a_file_template_is_read_and_its_resolvers_applied() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("my layout.json");
        std::fs::write(&path, r#"{
            "@timestamp": {"$resolver": "timestamp", "pattern": {"format": "yyyy-MM-dd HH:mm:ss,SSS XXX", "timeZone": "+01:00"}},
            "epochMillis": {"$resolver": "timestamp", "epoch": {"unit": "millis", "rounded": true}},
            "sev": {"$resolver": "level", "field": "severity", "severity": {"field": "code"}},
            "line": {"$resolver": "pattern", "pattern": "[%level] %logger - %msg"},
            "service": "rtdb",
            "seq": {"$resolver": "counter", "start": 10},
            "mdc": {"$resolver": "mdc", "flatten": {"prefix": "ctx."}}
        }"#).unwrap();
        let uri = format!("file://{}", path.display().to_string().replace(' ', "%20"));
        let template = JsonTemplate::load(&uri).unwrap();
        assert!(template.uses_counter());
        let context = [("database", "xtdb".to_owned())];
        let line = template.render(&event(Level::ERROR, &context));
        assert_eq!(line["@timestamp"], "2026-11-27 10:05:07,123 +01:00");
        assert_eq!(line["epochMillis"], 1_795_770_307_123i64);
        assert_eq!(line["sev"], 3);
        assert_eq!(line["line"], "[ERROR] xtdb.node.database - Slow");
        assert_eq!(line["service"], "rtdb");
        assert_eq!(line["seq"], 13);
        assert_eq!(line["ctx.database"], "xtdb");
        assert_eq!(line.as_object().unwrap().keys().next().map(String::as_str), Some("@timestamp"));
    }

    #[test]
    fn templates_rtdb_cannot_honour_refuse_startup() {
        for uri in ["classpath:EcsLayout.json", "https://example.com/layout.json", "file:relative.json", "file:///nonexistent/layout.json"] {
            assert_eq!(JsonTemplate::load(uri).unwrap_err().code, "rtdb.logging/json-template", "{uri}");
        }
        for template in [
            r#"{"x": {"$resolver": "map", "key": "a"}}"#,
            r#"{"x": {"$resolver": "thread", "field": "id"}}"#,
            r#"{"x": {"$resolver": "pattern", "pattern": "%X{a}"}}"#,
            r#"{"x": {"$resolver": "timestamp", "pattern": {"format": "yyyy G"}}}"#,
            r#"{"x": {"$resolver": "timestamp", "pattern": {"timeZone": "Europe/Paris"}}}"#,
            r#"[1]"#,
        ] {
            assert_eq!(JsonTemplate::parse("file:///t.json", template).unwrap_err().code, "rtdb.logging/json-template", "{template}");
        }
    }

    #[test]
    fn the_gcp_function_drops_an_unknown_location() {
        let parts = pattern("%replace{%C.%M}{^\\?\\.$}{}").unwrap();
        let mut unknown = event(Level::INFO, &[]);
        unknown.module = None;
        let mut out = String::new();
        render_parts(&parts, &unknown, &mut out);
        assert_eq!(out, "");
        assert_eq!(whole_literal("^\\?\\.$").as_deref(), Some("?."));
    }
}
