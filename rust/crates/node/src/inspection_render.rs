use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::transit::TransitValue;
use rtdb_tx::OperationValue;
use rtdb_types::{Anomaly, DecimalValue, Result, TimeUnit, Value};

pub(crate) fn quoted(text: &str) -> String { serde_json::to_string(text).expect("string serialization") }
pub(crate) fn map(fields: impl IntoIterator<Item = (&'static str, String)>) -> String {
    format!("{{{}}}", fields.into_iter().map(|(name, value)| format!(":{name} {value}")).collect::<Vec<_>>().join(", "))
}
pub(crate) fn vector(values: impl IntoIterator<Item = String>) -> String { format!("[{}]", values.into_iter().collect::<Vec<_>>().join(" ")) }
pub(crate) fn instant(micros: i64) -> String { format!("#time/instant {}", quoted(&datetime(i128::from(micros) * 1000, true))) }

pub(crate) struct OperationText(Option<CellWriter>);

impl OperationText {
    pub(crate) fn as_str(&self) -> &str {
        self.0.as_ref().map_or("<error decoding tx-ops>", |writer| writer.text.as_str())
    }
}

pub(crate) fn operations(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<OperationText> {
    let mut writer = None;
    let mut status = Ok(());
    let decoded = rtdb_tx::visit_operation_rows(bytes, memory, |row| {
        // A failed render still leaves the remaining Arrow values to validate.
        if status.is_err() { return; }
        status = append_operation(&mut writer, memory, row);
        if status.is_err() { writer = None; }
    });
    match decoded {
        Err(error) if error.code == "rtdb/memory-budget-exceeded" => return Err(error),
        Err(_) => return Ok(OperationText(None)),
        Ok(()) => {}
    }
    match status {
        Err(WriteError::Admission(error)) => Err(error),
        Err(WriteError::InvalidValue) => Ok(OperationText(None)),
        Ok(()) => {
            let writer = match writer {
                Some(mut writer) => {
                    writer.push("]").map_err(WriteError::into_anomaly)?;
                    writer
                }
                None => {
                    let mut writer = CellWriter::new(memory)?;
                    writer.push("[]").map_err(WriteError::into_anomaly)?;
                    writer
                }
            };
            Ok(OperationText(Some(writer)))
        }
    }
}

fn append_operation(writer: &mut Option<CellWriter>, memory: &Arc<MemoryBudget>, row: &OperationValue) -> WriteResult {
    let first = writer.is_none();
    if first { *writer = Some(CellWriter::new(memory)?); }
    let writer = writer.as_mut().expect("An operation row has a writer");
    writer.push(if first { "[" } else { " " })?;
    writer.operation(row)
}

enum WriteError {
    Admission(Anomaly),
    InvalidValue,
}

impl From<Anomaly> for WriteError {
    fn from(error: Anomaly) -> Self { Self::Admission(error) }
}

impl WriteError {
    fn into_anomaly(self) -> Anomaly {
        match self {
            Self::Admission(error) => error,
            Self::InvalidValue => invalid("Cannot render transaction operation"),
        }
    }
}

type WriteResult = std::result::Result<(), WriteError>;

struct CellWriter {
    text: String,
    reservation: Arc<MemoryReservation>,
}

impl CellWriter {
    fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        Ok(Self { text: String::new(), reservation: memory.reserve(128)? })
    }

    fn push(&mut self, value: &str) -> WriteResult {
        let required = self.text.len().checked_add(value.len()).filter(|&size| size <= i32::MAX as usize)
            .ok_or_else(|| invalid("Operation rendering exceeds addressable memory"))?;
        let capacity = self.text.capacity();
        if required > capacity {
            let preferred = required.saturating_add((capacity / 2).max(256)).min(i32::MAX as usize);
            let reserve = |capacity| self.reservation.budget().reserve(capacity + 128);
            // Reallocation can hold both the old and new buffer at once.
            let (capacity, reservation) = match reserve(preferred) {
                Err(_) if preferred > required => (required, reserve(required)?),
                result => (preferred, result?),
            };
            self.text.try_reserve_exact(capacity - self.text.len())
                .map_err(|error| Anomaly::incorrect("rtdb/allocation-failed", error.to_string()))?;
            self.reservation = reservation;
        }
        self.text.push_str(value);
        Ok(())
    }

    fn key(&mut self, name: &str) -> WriteResult {
        self.push(":")?;
        let name = if let Some(name) = name.strip_prefix('_') {
            self.push(if name.contains('$') { "xt." } else { "xt/" })?;
            name
        } else { name };
        let local = if let Some((namespace, local)) = name.rsplit_once('$') {
            for (index, segment) in namespace.split('$').enumerate() {
                self.separator(index, ".")?;
                self.key_segment(segment)?;
            }
            self.push("/")?;
            local
        } else { name };
        self.key_segment(local)
    }

    fn key_segment(&mut self, text: &str) -> WriteResult {
        let mut start = 0;
        for (index, _) in text.match_indices('_').filter(|(index, _)| *index != 0) {
            self.push(&text[start..index])?;
            self.push("-")?;
            start = index + 1;
        }
        self.push(&text[start..])
    }

    fn separator(&mut self, index: usize, separator: &str) -> WriteResult {
        if index != 0 { self.push(separator)?; }
        Ok(())
    }

    fn operation(&mut self, value: &OperationValue) -> WriteResult {
        match value {
            OperationValue::Scalar(value) => self.value(value),
            OperationValue::Tagged { tag, value } => {
                self.push("#xt/tagged [:")?;
                self.push(tag)?;
                self.push(" ")?;
                self.operation(value)?;
                self.push("]")
            }
            OperationValue::Struct(fields) => {
                self.push("{")?;
                for (index, (name, value)) in fields.iter().enumerate() {
                    self.separator(index, ", ")?;
                    self.key(name)?;
                    self.push(" ")?;
                    self.operation(value)?;
                }
                self.push("}")
            }
            OperationValue::List { values, set } => {
                self.push(if *set { "#{" } else { "[" })?;
                for (index, value) in values.iter().enumerate() {
                    self.separator(index, " ")?;
                    self.operation(value)?;
                }
                self.push(if *set { "}" } else { "]" })
            }
            OperationValue::Map(entries) => {
                self.push("{")?;
                for (index, (key, value)) in entries.iter().enumerate() {
                    self.separator(index, ", ")?;
                    self.operation(key)?;
                    self.push(" ")?;
                    self.operation(value)?;
                }
                self.push("}")
            }
        }
    }

    fn value(&mut self, value: &Value) -> WriteResult {
        match value {
            Value::List(_) | Value::FixedSizeList(_) | Value::Set(_) => {
                let set = matches!(value, Value::Set(_));
                self.push(if set { "#{" } else { "[" })?;
                for (index, value) in value.list_view().unwrap().iter().enumerate() {
                    self.separator(index, " ")?;
                    self.value(value.as_ref())?;
                }
                self.push(if set { "}" } else { "]" })
            }
            Value::Map { entries, .. } => {
                self.push("{")?;
                for (index, (key, value)) in entries.iter().enumerate() {
                    self.separator(index, ", ")?;
                    self.value(key)?;
                    self.push(" ")?;
                    self.value(value)?;
                }
                self.push("}")
            }
            Value::Struct(fields) => {
                self.push("{")?;
                for (index, (key, value)) in fields.iter().filter(|(_, value)| !matches!(value, Value::Null | Value::Undefined)).enumerate() {
                    self.separator(index, ", ")?;
                    self.key(key)?;
                    self.push(" ")?;
                    self.value(value)?;
                }
                self.push("}")
            }
            Value::TimestampTzRange { lower, upper } => {
                self.push("#xt/tstz-range [")?;
                self.endpoint(*lower, i64::MIN)?;
                self.push(" ")?;
                self.endpoint(*upper, i64::MAX)?;
                self.push("]")
            }
            Value::Transit(bytes) => {
                let _scratch = self.reservation.budget().reserve(rtdb_expr::text_allocation_bound(value)?)?;
                let transit = rtdb_format::transit::decode_msgpack(bytes)
                    .or_else(|_| rtdb_format::transit::decode_json(bytes)).map_err(|_| WriteError::InvalidValue)?;
                self.transit(&transit)
            }
            value => {
                let _scratch = self.reservation.budget().reserve(rtdb_expr::text_allocation_bound(value)?)?;
                let text = scalar(value).map_err(|_| WriteError::InvalidValue)?;
                self.push(&text)
            }
        }
    }

    fn endpoint(&mut self, value: i64, infinite: i64) -> WriteResult {
        if value == infinite { return self.push("nil"); }
        self.value(&Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) })
    }

    fn transit(&mut self, value: &TransitValue) -> WriteResult {
        match value {
            TransitValue::Array(values) => {
                self.push("[")?;
                for (index, value) in values.iter().enumerate() {
                    self.separator(index, " ")?;
                    self.transit(value)?;
                }
                self.push("]")
            }
            TransitValue::Map(entries) => {
                self.push("{")?;
                for (index, (key, value)) in entries.iter().enumerate() {
                    self.separator(index, ", ")?;
                    self.transit(key)?;
                    self.push(" ")?;
                    self.transit(value)?;
                }
                self.push("}")
            }
            TransitValue::Tagged { tag, representation } if tag == "r" => {
                let TransitValue::String(text) = representation.as_ref() else { return Err(WriteError::InvalidValue); };
                let uri = rtdb_types::UriParts::parse(text).map_err(|_| WriteError::InvalidValue)?;
                let _scratch = self.reservation.budget().reserve(uri.ascii_allocation_bound()?)?;
                self.push("#xt/uri ")?;
                self.push(&quoted(&uri.to_ascii_string()))
            }
            TransitValue::Tagged { tag, representation } => {
                self.push("#")?;
                self.push(tag)?;
                self.push(" ")?;
                self.transit(representation)
            }
            TransitValue::Null => self.push("nil"),
            TransitValue::Bool(value) => self.push(if *value { "true" } else { "false" }),
            TransitValue::Integer(value) => self.push(&value.to_string()),
            TransitValue::Float(value) => self.push(&float(*value)),
            TransitValue::Float32(value) => if value.is_finite() { self.push(&rtdb_types::java_f32_text(*value)) } else { self.push(&float(f64::from(*value))) },
            TransitValue::String(value) => self.push(&quoted(value)),
            TransitValue::Keyword(value) => { self.push(":")?; self.push(value) }
            TransitValue::Symbol(value) => self.push(value),
            TransitValue::Bytes(value) => { self.push("#bytes ")?; self.push(&quoted(&hex(value))) }
        }
    }
}
fn scalar(value: &Value) -> Result<String> {
    Ok(match value {
        Value::Undefined | Value::Null => "nil".into(), Value::Bool(value) => value.to_string(),
        Value::I8(value) => value.to_string(), Value::I16(value) => value.to_string(), Value::I32(value) => value.to_string(), Value::I64(value) => value.to_string(),
        Value::U8(value) => value.to_string(), Value::U16(value) => value.to_string(), Value::U32(value) => value.to_string(), Value::U64(value) => value.to_string(),
        Value::F16(bits) => {
            let sign = if bits & 0x8000 == 0 { 1.0 } else { -1.0 }; let exponent = (bits >> 10) & 31; let fraction = bits & 1023;
            float(if exponent == 31 { if fraction == 0 { sign * f64::INFINITY } else { f64::NAN } } else if exponent == 0 { sign * f64::from(fraction) * 2_f64.powi(-24) } else { sign * f64::from(1024 + fraction) * 2_f64.powi(i32::from(exponent) - 25) })
        }
        Value::F32(value) => if value.is_finite() { finite_float(value.to_string()) } else { float(f64::from(*value)) }, Value::F64(value) => float(*value),
        Value::Utf8(value) => quoted(value), Value::Keyword(value) => format!(":{}", value.as_str()),
        Value::Binary(bytes) | Value::FixedSizeBinary(bytes) => format!("#bytes {}", quoted(&hex(bytes))),
        Value::Uuid(bytes) => { let hex = hex(bytes); format!("#uuid {}", quoted(&format!("{}-{}-{}-{}-{}", &hex[..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..]))) }
        Value::Uri(value) => format!("#xt/uri {}", quoted(&rtdb_types::UriParts::parse(value)?.to_ascii_string())),
        Value::Oid(value) => format!("#xt/oid {value}"), Value::RegClass(value) => format!("#xt/regclass {value}"), Value::RegProc(value) => format!("#xt/regproc {value}"),
        Value::Decimal(value) => decimal(value),
        Value::Date(value) => format!("#time/date {}", quoted(&value.date().to_string())),
        Value::DateDay(days) => format!("#time/date {}", quoted(&date(i64::from(*days)))),
        Value::DateMilli(millis) => format!("#time/date {}", quoted(&date(millis.div_euclid(rtdb_types::java_date::MILLIS_PER_DAY)))),
        Value::Time { value, unit, .. } => format!("#time/time {}", quoted(&time(nanos(*value, *unit), false))),
        Value::Timestamp { value, unit, timezone } => {
            let nanos = nanos(*value, *unit);
            if let Some(zone) = timezone {
                let text = zoned(nanos, zone)?;
                format!("#time/zoned-date-time {}", quoted(&text))
            } else { rtdb_types::CivilDateTime::from_epoch_nanos(nanos)?; format!("#time/date-time {}", quoted(&datetime(nanos, false))) }
        }
        Value::Duration { value, unit } => format!("#time/duration {}", quoted(&duration(nanos(*value, *unit)))),
        Value::IntervalYearMonth(months) => interval(*months, 0, 0),
        Value::IntervalDayTime { days, milliseconds } => interval(0, *days, i128::from(*milliseconds) * 1_000_000),
        Value::IntervalMonthDayNano { months, days, nanoseconds } | Value::IntervalMonthDayMicro { months, days, nanoseconds } => interval(*months, *days, i128::from(*nanoseconds)),
        _ => unreachable!("Operation containers are written incrementally"),
    })
}
fn float(value: f64) -> String {
    if value.is_nan() { "##NaN".into() } else if value == f64::INFINITY { "##Inf".into() } else if value == f64::NEG_INFINITY { "##-Inf".into() } else { finite_float(value.to_string()) }
}
fn finite_float(text: String) -> String { if text.contains(['.', 'e', 'E']) { text } else { format!("{text}.0") } }
fn hex(bytes: &[u8]) -> String { bytes.iter().map(|byte| format!("{byte:02x}")).collect() }
fn decimal(value: &DecimalValue) -> String {
    format!("{}M", rtdb_expr::decimal_text(value))
}
fn nanos(value: i64, unit: TimeUnit) -> i128 { i128::from(value) * match unit { TimeUnit::Second => 1_000_000_000, TimeUnit::Millisecond => 1_000_000, TimeUnit::Microsecond => 1000, TimeUnit::Nanosecond => 1 } }
fn zoned(nanos: i128, zone: &str) -> Result<String> {
    let zone = rtdb_expr::normalize_timezone(zone)?;
    let seconds = i64::try_from(nanos.div_euclid(1_000_000_000)).map_err(|_| invalid("Timestamp exceeds formatter range"))?;
    let offset = rtdb_types::ZoneId::parse(&zone)?.offset_at(seconds)?;
    let local = nanos + i128::from(offset.seconds()) * 1_000_000_000;
    rtdb_types::CivilDateTime::from_epoch_nanos(local)?;
    let bracket = !(zone == "Z" || zone.starts_with(['+', '-']));
    Ok(format!("{}{}{}", datetime(local, false), offset, if bracket { format!("[{zone}]") } else { String::new() }))
}
fn time(nanos: i128, seconds_required: bool) -> String {
    let seconds = nanos.div_euclid(1_000_000_000);
    let fraction = nanos.rem_euclid(1_000_000_000);
    let mut value = format!("{:02}:{:02}", seconds / 3600, seconds / 60 % 60);
    if seconds_required || seconds % 60 != 0 || fraction != 0 { value.push_str(&format!(":{:02}", seconds % 60)); }
    if fraction != 0 { let digits = if fraction % 1_000_000 == 0 { 3 } else if fraction % 1000 == 0 { 6 } else { 9 }; value.push('.'); value.push_str(&format!("{fraction:09}")[..digits]); }
    value
}
fn datetime(nanos: i128, utc: bool) -> String {
    let days = nanos.div_euclid(86_400_000_000_000);
    format!("{}T{}{}", date(days as i64), time(nanos.rem_euclid(86_400_000_000_000), utc), if utc { "Z" } else { "" })
}
pub(crate) fn date(days: i64) -> String {
    rtdb_types::CivilDate::from_epoch_day(days).expect("validated calendar date or physical Arrow date").to_string()
}
fn duration(nanos: i128) -> String {
    if nanos == 0 { return "PT0S".into(); }
    let sign = if nanos < 0 { "-" } else { "" }; let nanos = nanos.abs();
    let seconds = nanos / 1_000_000_000;
    let mut text = "PT".to_owned();
    if seconds >= 3600 { text.push_str(&format!("{sign}{}H", seconds / 3600)); }
    if seconds / 60 % 60 != 0 { text.push_str(&format!("{sign}{}M", seconds / 60 % 60)); }
    if seconds % 60 != 0 || nanos % 1_000_000_000 != 0 { text.push_str(&format!("{sign}{}", seconds % 60)); if nanos % 1_000_000_000 != 0 { text.push('.'); text.push_str(format!("{:09}", nanos % 1_000_000_000).trim_end_matches('0')); } text.push('S'); }
    text
}
fn interval(months: i32, days: i32, nanos: i128) -> String {
    let mut period = "P".to_owned();
    if months / 12 != 0 { period.push_str(&format!("{}Y", months / 12)); }
    if months % 12 != 0 { period.push_str(&format!("{}M", months % 12)); }
    if days != 0 { period.push_str(&format!("{days}D")); }
    if nanos != 0 { period.push_str(&duration(nanos)[1..]); }
    format!("#xt/interval {}", quoted(&period))
}
fn invalid(message: &str) -> Anomaly { Anomaly::incorrect("rtdb.node/inspection-value", message) }

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    fn operation(value: &OperationValue) -> String {
        let mut writer = CellWriter::new(&MemoryBudget::new(1 << 20)).unwrap();
        writer.operation(value).map_err(WriteError::into_anomaly).unwrap();
        writer.text
    }

    fn render(value: &Value) -> Result<String> {
        let mut writer = CellWriter::new(&MemoryBudget::new(1 << 20))?;
        writer.value(value).map_err(WriteError::into_anomaly)?;
        Ok(writer.text)
    }

    #[test]
    fn operation_cell_keeps_batch_and_field_order_and_preflights_the_end_marker() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("z", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("a", arrow_schema::DataType::Utf8, true),
        ]));
        let page = |number: i64, text: Option<&str>| RecordBatch::try_new(Arc::clone(&schema), vec![
            Arc::new(Int64Array::from(vec![number])),
            Arc::new(StringArray::from(vec![text])),
        ]).unwrap();
        let first = page(1, Some("one"));
        let second = page(2, None);
        let bytes = rtdb_format::ipc::write_stream_to(&schema, [&first, &second], Vec::new()).unwrap();
        let memory = MemoryBudget::new(1 << 20);
        let text = operations(&bytes, &memory).unwrap();
        assert_eq!(text.as_str(), "[{:a \"one\", :z 1} {:z 2}]");
        assert!(memory.used_bytes() > 0);
        drop(text);
        assert_eq!(memory.used_bytes(), 0);
        assert_eq!(operations(&[bytes.as_slice(), &[0]].concat(), &memory).unwrap().as_str(), "<error decoding tx-ops>");
        let Err(error) = operations(&bytes, &MemoryBudget::new(0)) else { panic!("admitted an unfunded stream"); };
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    }

    #[test]
    fn operation_cell_growth_keeps_its_old_buffer_funded() {
        let memory = MemoryBudget::new(259);
        let mut writer = CellWriter::new(&memory).unwrap();
        writer.push("abc").map_err(WriteError::into_anomaly).unwrap();
        assert_eq!(memory.used_bytes(), 131);
        let Err(WriteError::Admission(error)) = writer.push("d") else { panic!("admitted unfunded growth"); };
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(writer.text, "abc");
    }

    #[test]
    fn operation_uri_rendering_reserves_nfc_scratch_and_decodes_transit_uri_objects() {
        let uri = Value::Uri("/e\u{301}".into());
        let operation_value = OperationValue::Scalar(uri.clone());
        let bound = rtdb_expr::text_allocation_bound(&uri).unwrap();
        assert!(bound >= rtdb_types::UriParts::parse("/e\u{301}").unwrap().ascii_allocation_bound().unwrap());
        assert_eq!(operation(&operation_value),"#xt/uri \"/%C3%A9\"");
        let tagged = TransitValue::Tagged { tag: "r".into(), representation: Box::new(TransitValue::String("/e\u{301}".into())) };
        let value = Value::Transit(rtdb_format::transit::encode_msgpack(&tagged).unwrap());
        assert_eq!(render(&value).unwrap(),render(&uri).unwrap());
    }

    #[test]
    fn log_tables_edn_preserves_tagged_operations_keyword_names_and_negative_instants() {
        let value = rtdb_tx::OperationValue::Tagged { tag: "put-docs".into(), value: Box::new(rtdb_tx::OperationValue::Struct([
            ("_id".into(), rtdb_tx::OperationValue::Scalar(Value::I64(1))),
            ("foo_bar$baz_qux".into(), rtdb_tx::OperationValue::Scalar(Value::Utf8("v".into()))),
        ].into())) };
        assert_eq!(operation(&value), "#xt/tagged [:put-docs {:xt/id 1, :foo-bar/baz-qux \"v\"}]");
        assert_eq!(instant(-1), "#time/instant \"1969-12-31T23:59:59.999999Z\"");
        assert_eq!(instant(0), "#time/instant \"1970-01-01T00:00:00Z\"");
        assert_eq!(render(&Value::DateMilli(-1)).unwrap(), "#time/date \"1969-12-31\"");
        assert_eq!(render(&Value::DateMilli(-86_400_001)).unwrap(), "#time/date \"1969-12-30\"");
        assert_eq!(render(&Value::Time { value: 0, unit: TimeUnit::Microsecond, bit_width: 64 }).unwrap(), "#time/time \"00:00\"");
        assert_eq!(render(&Value::TimestampTzRange { lower: 0, upper: i64::MAX }).unwrap(), "#xt/tstz-range [#time/zoned-date-time \"1970-01-01T00:00Z[UTC]\" nil]");
        assert_eq!(render(&Value::F32(0.1)).unwrap(), "0.1");
        assert_eq!(render(&Value::Timestamp { value: 1, unit: TimeUnit::Nanosecond, timezone: Some("+01:00:01".into()) }).unwrap(), "#time/zoned-date-time \"1970-01-01T01:00:01.000000001+01:00:01\"");
        assert_eq!(render(&Value::Timestamp { value: 0, unit: TimeUnit::Microsecond, timezone: Some("Europe/Paris".into()) }).unwrap(), "#time/zoned-date-time \"1970-01-01T01:00+01:00[Europe/Paris]\"");
        assert_eq!(decimal(&DecimalValue { unscaled: vec![123], precision: 3, scale: -2, bit_width: 128 }), "1.23E+4M");
        assert_eq!(decimal(&DecimalValue { unscaled: vec![123], precision: 3, scale: i32::MIN, bit_width: 128 }), "1.23E+2147483650M");
        assert_eq!(decimal(&DecimalValue { unscaled: vec![123], precision: 3, scale: i32::MAX, bit_width: 128 }), "1.23E-2147483645M");
        assert_eq!(render(&Value::IntervalMonthDayMicro { months: 13, days: -2, nanoseconds: 1_500_000_000 }).unwrap(), "#xt/interval \"P1Y1M-2DT1.5S\"");
    }
}
