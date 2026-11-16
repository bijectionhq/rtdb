use std::{collections::BTreeMap, sync::Arc};
use chrono::{Datelike, Timelike};
use rtdb_types::{Result, TimeUnit, Value};
use super::{ConnectData, ConnectSchema, SchemaKind, SinkRecord, Transform, error, fields_with_defaults, required, string_map};

enum Target { String, Unix, Date, Time, Timestamp }
enum Precision { Seconds, Millis, Micros, Nanos }
struct TimestampConverter { key: bool, field: String, target: Target, precision: Precision, format: Option<DatePattern>, replace: bool }
struct TimestampRouter { topic: String, format: DatePattern }
pub(super) fn converter(options: &BTreeMap<String, String>, key: bool) -> Result<Box<dyn Transform>> {
    let target = match required(options, "target.type")? { "string" => Target::String, "unix" => Target::Unix, "Date" => Target::Date, "Time" => Target::Time, "Timestamp" => Target::Timestamp, _ => return Err(error("invalid-plugin-config", "Unknown timestamp target.type")) };
    let precision = match options.get("unix.precision").map_or("milliseconds", String::as_str) { "seconds" => Precision::Seconds, "milliseconds" => Precision::Millis, "microseconds" => Precision::Micros, "nanoseconds" => Precision::Nanos, _ => return Err(error("invalid-plugin-config", "Unknown unix.precision")) };
    let format = options.get("format").filter(|format| !format.trim().is_empty()).map(|format| DatePattern::parse(format)).transpose()?;
    if matches!(target, Target::String) && format.is_none() { return Err(error("invalid-plugin-config", "String timestamp output requires format")); }
    Ok(Box::new(TimestampConverter { key, field: options.get("field").cloned().unwrap_or_default(), target, precision, format, replace: super::super::converter::option_bool(options, "replace.null.with.default", true)? }))
}
pub(super) fn router(options: &BTreeMap<String, String>) -> Result<Box<dyn Transform>> {
    Ok(Box::new(TimestampRouter { topic: options.get("topic.format").cloned().unwrap_or_else(|| "${topic}-${timestamp}".into()), format: DatePattern::parse(options.get("timestamp.format").map_or("yyyyMMdd", String::as_str))? }))
}
impl Transform for TimestampRouter {
    fn allocation_bound(&self, record: &SinkRecord) -> Result<usize> {
        let topic_slots = self.topic.matches("${topic}").count(); let time_slots = self.topic.matches("${timestamp}").count().saturating_add(topic_slots.saturating_mul(record.topic.matches("${timestamp}").count()));
        let bound = self.topic.len().checked_add(topic_slots.checked_mul(record.topic.len()).ok_or_else(|| error("allocation-overflow", "Topic routing allocation overflow"))?).and_then(|bytes| bytes.checked_add(time_slots.checked_mul(self.format.maximum_output_bytes())?)).and_then(|bytes| bytes.checked_mul(4)).ok_or_else(|| error("allocation-overflow", "Topic routing allocation overflow"))?;
        record.retained_bytes()?.checked_mul(4).and_then(|bytes| bytes.checked_add(bound)).ok_or_else(|| error("allocation-overflow", "Topic routing allocation overflow"))
    }
    fn apply(&mut self, mut record: SinkRecord) -> Result<Option<SinkRecord>> {
        let timestamp = record.timestamp_millis.ok_or_else(|| error("transform-error", "TimestampRouter requires a record timestamp"))?;
        record.topic = self.topic.replace("${topic}", &record.topic).replace("${timestamp}", &self.format.format(timestamp)?);
        Ok(Some(record))
    }
}
impl TimestampConverter {
    fn schema(&self, optional: bool) -> Arc<ConnectSchema> {
        let (kind, logical) = match self.target { Target::String => (SchemaKind::String, None), Target::Unix => (SchemaKind::I64, None), Target::Date => (SchemaKind::I32, Some("Date")), Target::Time => (SchemaKind::I32, Some("Time")), Target::Timestamp => (SchemaKind::I64, Some("Timestamp")) };
        let mut schema = ConnectSchema::new(kind); schema.optional = optional; schema.name = logical.map(|name| format!("org.apache.kafka.connect.data.{name}")); if logical.is_some() { schema.version = Some(1); } Arc::new(schema)
    }
    fn convert(&self, value: &Value, schema: Option<&ConnectSchema>) -> Result<Value> {
        let schema = schema.map(ConnectSchema::resolved); let schema = schema.as_deref();
        if let Some(schema) = schema {
            if !matches!(schema.name.as_deref(), Some("org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time" | "org.apache.kafka.connect.data.Timestamp")) && !matches!(schema.kind, SchemaKind::I64 | SchemaKind::String) { return Err(error("transform-error", "Schema does not identify a timestamp format")); }
        }
        if *value == Value::Null { return Ok(Value::Null); }
        let millis = match value {
            Value::I64(value) => match self.precision { Precision::Seconds => value.saturating_mul(1000), Precision::Millis => *value, Precision::Micros => *value / 1000, Precision::Nanos => *value / 1_000_000 },
            Value::Utf8(value) => self.format.as_ref().ok_or_else(|| error("transform-error", "String timestamp input requires format"))?.read(value)?,
            Value::DateDay(value) => i64::from(*value) * 86_400_000,
            Value::Time { value, unit: TimeUnit::Millisecond, .. } | Value::Timestamp { value, unit: TimeUnit::Millisecond, .. } => *value,
            _ => return Err(error("transform-error", "Value is not a supported timestamp representation")),
        };
        Ok(match self.target {
            Target::String => Value::Utf8(self.format.as_ref().ok_or_else(|| error("transform-error", "String timestamp output requires format"))?.format(millis)?),
            Target::Unix => Value::I64(match self.precision { Precision::Seconds => millis / 1000, Precision::Millis => millis, Precision::Micros => millis.saturating_mul(1000), Precision::Nanos => millis.saturating_mul(1_000_000) }),
            Target::Date => Value::DateDay(i32::try_from(millis.div_euclid(86_400_000)).map_err(|cause| error("transform-error", cause))?),
            Target::Time => Value::Time { value: millis.rem_euclid(86_400_000), unit: TimeUnit::Millisecond, bit_width: 32 },
            Target::Timestamp => Value::Timestamp { value: millis, unit: TimeUnit::Millisecond, timezone: Some("UTC".into()) },
        })
    }
    fn fields(&self, schema: &ConnectSchema, value: &Value) -> Result<Value> {
        let schema = schema.resolved();
        if *value == Value::Null { return Ok(Value::Null); }
        let data = ConnectData { schema: Some(Arc::new(schema.as_ref().clone())), value: value.clone() };
        let mut values = fields_with_defaults(&data, self.replace)?;
        if let Some(field) = schema.field(&self.field) { values.insert(self.field.clone(), self.convert(values.get(&self.field).unwrap_or(&Value::Null), Some(&field.schema))?); }
        Ok(Value::Struct(values))
    }
}
impl Transform for TimestampConverter {
    fn apply(&mut self, mut record: SinkRecord) -> Result<Option<SinkRecord>> {
        let data = if self.key { &mut record.key } else { &mut record.value };
        super::resolve_record_schema(data);
        if self.field.is_empty() {
            let value = self.convert(&data.value, data.schema.as_deref())?;
            let schema = data.schema.as_ref().map(|schema| self.schema(schema.optional));
            *data = ConnectData { schema, value };
        } else if let Some(schema) = &data.schema {
            let value = self.fields(schema, &data.value)?;
            let mut updated = schema.as_ref().clone();
            let SchemaKind::Struct(fields) = &mut updated.kind else { return Err(error("transform-error", "Field TimestampConverter requires a Struct")); };
            for field in fields { if field.name == self.field { field.schema = self.schema(field.schema.optional); } }
            updated.default = schema.default.as_ref().map(|value| self.fields(schema, value)).transpose()?;
            *data = ConnectData { schema: Some(Arc::new(updated)), value };
        } else if data.value != Value::Null {
            let mut values = fields_with_defaults(data, self.replace)?;
            values.insert(self.field.clone(), self.convert(values.get(&self.field).unwrap_or(&Value::Null), None)?);
            data.value = string_map(values);
        }
        Ok(Some(record))
    }
}

struct DatePattern { parts: Vec<Part> }
enum Part { Literal(String), Field(char, usize) }
impl DatePattern {
    fn parse(text: &str) -> Result<Self> {
        let mut parts = Vec::new(); let mut literal = String::new(); let mut chars = text.chars().peekable(); let mut quoted = false;
        while let Some(character) = chars.next() {
            if character == '\'' {
                if chars.peek() == Some(&'\'') { chars.next(); literal.push('\''); } else { quoted = !quoted; } continue;
            }
            if !quoted && character.is_ascii_alphabetic() {
                if !literal.is_empty() { parts.push(Part::Literal(std::mem::take(&mut literal))); }
                let mut count = 1; while chars.peek() == Some(&character) { chars.next(); count += 1; }
                if !matches!(character, 'y' | 'M' | 'd' | 'H' | 'm' | 's' | 'S' | 'Z' | 'X') || (character == 'X' && count > 3) {
                    return Err(error("invalid-plugin-config", format!("Native date pattern does not support field '{character}' with width {count}")));
                }
                parts.push(Part::Field(character, count));
            } else { literal.push(character); }
        }
        if quoted { return Err(error("invalid-plugin-config", "Unterminated quoted date pattern")); }
        if !literal.is_empty() { parts.push(Part::Literal(literal)); } Ok(Self { parts })
    }
    fn maximum_output_bytes(&self) -> usize { self.parts.iter().map(|part| match part { Part::Literal(value) => value.len(), Part::Field(_, width) => (*width).max(32) }).sum() }
    fn format(&self, millis: i64) -> Result<String> {
        let date = chrono::DateTime::from_timestamp_millis(millis).ok_or_else(|| error("transform-error", "Timestamp is outside the native date formatting range"))?;
        let mut result = String::new();
        for part in &self.parts {
            match part {
                Part::Literal(value) => result.push_str(value),
                Part::Field('Z', _) => result.push_str("+0000"), Part::Field('X', _) => result.push('Z'),
                Part::Field('M', count) if *count >= 3 => result.push_str(if *count == 3 { MONTHS_SHORT[date.month0() as usize] } else { MONTHS_LONG[date.month0() as usize] }),
                Part::Field(field, count) => {
                    let value = match field { 'y' if *count == 2 => i64::from(date.year().rem_euclid(100)), 'y' => i64::from(date.year()), 'M' => i64::from(date.month()), 'd' => i64::from(date.day()), 'H' => i64::from(date.hour()), 'm' => i64::from(date.minute()), 's' => i64::from(date.second()), 'S' => i64::from(date.timestamp_subsec_millis()), _ => return Err(error("transform-error", "Unknown date pattern field")) };
                    result.push_str(&format!("{value:0width$}", width = *count));
                }
            }
        }
        Ok(result)
    }
    fn read(&self, text: &str) -> Result<i64> {
        let mut rest = text; let mut year = 1970i64; let mut month = 1i64; let mut day = 1i64; let mut hour = 0i64; let mut minute = 0i64; let mut second = 0i64; let mut millis = 0i64; let mut zone = 0i64;
        for (index, part) in self.parts.iter().enumerate() {
            match part {
                Part::Literal(value) => rest = rest.strip_prefix(value).ok_or_else(|| error("transform-error", "Timestamp does not match its format"))?,
                Part::Field(field @ ('Z' | 'X'), count) => {
                    if let Some(tail) = rest.strip_prefix('Z') { rest = tail; continue; }
                    let sign = if let Some(tail) = rest.strip_prefix('+') { rest = tail; 1 } else if let Some(tail) = rest.strip_prefix('-') { rest = tail; -1 } else { return Err(error("transform-error", "Timestamp zone is missing")); };
                    let hours = digits(&mut rest, Some(2))?;
                    let minutes = if *field == 'X' && *count == 1 { 0 } else { if *field == 'X' && *count == 3 { rest = rest.strip_prefix(':').ok_or_else(|| error("transform-error", "Timestamp zone lacks colon"))?; } digits(&mut rest, Some(2))? };
                    if hours > 23 || minutes > 59 { return Err(error("transform-error", "Invalid timestamp zone")); } zone = sign * (hours * 60 + minutes) * 60_000;
                }
                Part::Field('M', count) if *count >= 3 => {
                    let names = if *count == 3 { MONTHS_SHORT } else { MONTHS_LONG };
                    let (index, name) = names.iter().enumerate().find(|(_, name)| rest.get(..name.len()).is_some_and(|value| value.eq_ignore_ascii_case(name))).ok_or_else(|| error("transform-error", "Unknown timestamp month name"))?;
                    month = index as i64 + 1; rest = &rest[name.len()..];
                }
                Part::Field(field, count) => {
                    if *field == 'y' && *count == 2 { return Err(error("transform-error", "Two-digit year parsing requires a configured century pivot")); }
                    let adjacent = matches!(self.parts.get(index + 1), Some(Part::Field(next, _)) if !matches!(next, 'Z' | 'X'));
                    let value = digits(&mut rest, adjacent.then_some(*count))?;
                    match field { 'y' => year = value, 'M' => month = value, 'd' => day = value, 'H' => hour = value, 'm' => minute = value, 's' => second = value, 'S' => millis = value, _ => return Err(error("transform-error", "Unknown date pattern field")) }
                }
            }
        }
        let month0 = month.checked_sub(1).ok_or_else(|| error("transform-error", "Timestamp month overflow"))?;
        let year = year.checked_add(month0.div_euclid(12)).and_then(|year| i32::try_from(year).ok()).ok_or_else(|| error("transform-error", "Timestamp year overflow"))?;
        let base = chrono::NaiveDate::from_ymd_opt(year, month0.rem_euclid(12) as u32 + 1, 1).and_then(|date| date.and_hms_opt(0, 0, 0)).ok_or_else(|| error("transform-error", "Timestamp date is outside its range"))?.and_utc().timestamp_millis();
        let delta = i128::from(day - 1) * 86_400_000 + i128::from(hour) * 3_600_000 + i128::from(minute) * 60_000 + i128::from(second) * 1000 + i128::from(millis) - i128::from(zone);
        i64::try_from(i128::from(base) + delta).map_err(|cause| error("transform-error", cause))
    }
}
fn digits(rest: &mut &str, width: Option<usize>) -> Result<i64> {
    let end = rest.bytes().take_while(u8::is_ascii_digit).count();
    let count = width.unwrap_or(end);
    if count == 0 || end < count { return Err(error("transform-error", "Timestamp numeric field is missing")); }
    let value = rest[..count].parse().map_err(|cause| error("transform-error", cause))?; *rest = &rest[count..]; Ok(value)
}
const MONTHS_SHORT: [&str; 12] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const MONTHS_LONG: [&str; 12] = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unix_precision_truncates_toward_zero_and_saturates_like_java_timeunit() {
        let converter = TimestampConverter { key: false, field: String::new(), target: Target::Timestamp, precision: Precision::Micros, format: None, replace: true };
        assert_eq!(converter.convert(&Value::I64(-1999), None).unwrap(), Value::Timestamp { value: -1, unit: TimeUnit::Millisecond, timezone: Some("UTC".into()) });
        let converter = TimestampConverter { precision: Precision::Seconds, ..converter };
        assert_eq!(converter.convert(&Value::I64(i64::MAX), None).unwrap(), Value::Timestamp { value: i64::MAX, unit: TimeUnit::Millisecond, timezone: Some("UTC".into()) });
    }
    #[test]
    fn date_pattern_preserves_quotes_width_lenient_day_and_zone() {
        let pattern = DatePattern::parse("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").unwrap();
        assert_eq!(pattern.read("1970-01-32T01:00:00.001+01:00").unwrap(), 31 * 86_400_000 + 1);
        assert_eq!(pattern.format(1).unwrap(), "1970-01-01T00:00:00.001Z");
        assert_eq!(DatePattern::parse("yyyyMMdd").unwrap().format(86_400_000).unwrap(), "19700102");
    }
}
