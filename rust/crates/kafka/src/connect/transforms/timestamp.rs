use std::{collections::{BTreeMap,VecDeque}, sync::Arc};
use rtdb_types::{Result, TimeUnit, Value};
use super::{ConnectData, ConnectSchema, SchemaKind, SinkRecord, Transform, error, fields_with_defaults, required, string_map};

use super::super::{DateEnvironment, DatePattern};

enum Target { String, Unix, Date, Time, Timestamp }
enum Precision { Seconds, Millis, Micros, Nanos }
struct TimestampConverter { key: bool, field: String, target: Target, precision: Precision, format: Option<DatePattern>, replace: bool, environment: Arc<DateEnvironment>, schemas: VecDeque<CachedSchema> }
struct CachedSchema { source: Arc<ConnectSchema>,output: Arc<ConnectSchema>,charge: Arc<rtdb_columnar::MemoryReservation> }
struct TimestampRouter { topic: String, format: RouterFormat }
enum RouterFormat { Uninitialized { pattern: String,environment: Arc<DateEnvironment> },Ready(DatePattern) }
impl RouterFormat {
    fn maximum_output_bytes(&self) -> Result<usize> {
        match self { Self::Uninitialized { pattern,environment } => DatePattern::uncompiled_output_bound(pattern,environment),Self::Ready(format) => Ok(format.maximum_output_bytes()) }
    }
    fn get(&mut self) -> Result<&mut DatePattern> {
        if let Self::Uninitialized { pattern,environment } = self {
            let compiled = DatePattern::compile(pattern,Arc::clone(environment),true)?;
            *self = Self::Ready(compiled);
        }
        let Self::Ready(format) = self else { unreachable!() };Ok(format)
    }
}
pub(super) fn converter(options: &BTreeMap<String, String>, key: bool, environment: Arc<DateEnvironment>) -> Result<Box<dyn Transform>> {
    let target = match required(options, "target.type")? { "string" => Target::String, "unix" => Target::Unix, "Date" => Target::Date, "Time" => Target::Time, "Timestamp" => Target::Timestamp, _ => return Err(error("invalid-plugin-config", "Unknown timestamp target.type")) };
    let precision = match options.get("unix.precision").map_or("milliseconds", String::as_str) { "seconds" => Precision::Seconds, "milliseconds" => Precision::Millis, "microseconds" => Precision::Micros, "nanoseconds" => Precision::Nanos, _ => return Err(error("invalid-plugin-config", "Unknown unix.precision")) };
    let format = options.get("format").filter(|format| format.bytes().any(|byte|byte > 0x20)).map(|format| DatePattern::compile(format,Arc::clone(&environment),true)).transpose()?;
    if matches!(target, Target::String) && format.is_none() { return Err(error("invalid-plugin-config", "String timestamp output requires format")); }
    Ok(Box::new(TimestampConverter { key, field: options.get("field").cloned().unwrap_or_default(), target, precision, format, replace: super::super::converter::option_bool(options, "replace.null.with.default", true)?,environment,schemas: VecDeque::new() }))
}
pub(super) fn router(options: &BTreeMap<String, String>, environment: Arc<DateEnvironment>) -> Result<Box<dyn Transform>> {
    Ok(Box::new(TimestampRouter { topic: options.get("topic.format").cloned().unwrap_or_else(|| "${topic}-${timestamp}".into()), format: RouterFormat::Uninitialized { pattern: options.get("timestamp.format").map_or("yyyyMMdd",String::as_str).into(),environment } }))
}
impl Transform for TimestampRouter {
    fn allocation_bound(&self, record: &SinkRecord) -> Result<usize> {
        let topic_slots = self.topic.matches("${topic}").count(); let time_slots = self.topic.matches("${timestamp}").count().saturating_add(topic_slots.saturating_mul(record.topic.matches("${timestamp}").count()));
        let maximum = self.format.maximum_output_bytes()?;
        let bound = self.topic.len().checked_add(topic_slots.checked_mul(record.topic.len()).ok_or_else(|| error("allocation-overflow", "Topic routing allocation overflow"))?).and_then(|bytes| bytes.checked_add(time_slots.checked_mul(maximum)?)).and_then(|bytes| bytes.checked_add(maximum)).and_then(|bytes| bytes.checked_mul(4)).ok_or_else(|| error("allocation-overflow", "Topic routing allocation overflow"))?;
        record.retained_bytes()?.checked_mul(4).and_then(|bytes| bytes.checked_add(bound)).ok_or_else(|| error("allocation-overflow", "Topic routing allocation overflow"))
    }
    fn apply(&mut self, mut record: SinkRecord) -> Result<Option<SinkRecord>> {
        let timestamp = record.timestamp_millis.ok_or_else(|| error("transform-error", "TimestampRouter requires a record timestamp"))?;
        let formatted = self.format.get()?.format(timestamp)?;
        record.topic = self.topic.replace("${topic}", &record.topic).replace("${timestamp}", &formatted);
        Ok(Some(record))
    }
}
impl TimestampConverter {
    fn updated_schema(&mut self,source: &Arc<ConnectSchema>) -> Result<(Arc<ConnectSchema>,Arc<rtdb_columnar::MemoryReservation>)> {
        if let Some(index) = self.schemas.iter().position(|cached|cached.source == *source) {
            let cached = self.schemas.remove(index).unwrap();
            let result = (Arc::clone(&cached.output),Arc::clone(&cached.charge));self.schemas.push_back(cached);return Ok(result);
        }
        let bound = source.retained_bytes()?.checked_mul(8).and_then(|bytes|bytes.checked_add(self.format.as_ref().map_or(0,DatePattern::maximum_output_bytes).checked_mul(4)?)).and_then(|bytes|bytes.checked_add(4096))
            .ok_or_else(||error("allocation-overflow","Timestamp schema cache allocation exceeds range"))?;
        let charge = self.environment.reserve(bound)?;
        let mut updated = source.as_ref().clone();
        let SchemaKind::Struct(fields) = &mut updated.kind else { return Err(error("transform-error","Field TimestampConverter requires a Struct")); };
        for field in fields { if field.name == self.field { field.schema = self.schema(field.schema.optional); } }
        updated.default = source.default_value().map(|value|self.fields(source,&updated,value)).transpose()?;
        if let Some(default) = &updated.default { super::super::data::validate_value(&updated,default,0)?; }
        let output = Arc::new(updated);
        if self.schemas.len() == 16 { self.schemas.pop_front(); }
        self.schemas.push_back(CachedSchema { source: Arc::clone(source),output: Arc::clone(&output),charge: Arc::clone(&charge) });
        Ok((output,charge))
    }
    fn schema(&self, optional: bool) -> Arc<ConnectSchema> {
        let (kind, logical) = match self.target { Target::String => (SchemaKind::String, None), Target::Unix => (SchemaKind::I64, None), Target::Date => (SchemaKind::I32, Some("Date")), Target::Time => (SchemaKind::I32, Some("Time")), Target::Timestamp => (SchemaKind::I64, Some("Timestamp")) };
        let mut schema = ConnectSchema::new(kind); schema.optional = optional; schema.name = logical.map(|name| format!("org.apache.kafka.connect.data.{name}")); if logical.is_some() { schema.version = Some(1); } Arc::new(schema)
    }
    fn convert(&mut self, value: &Value, schema: Option<&ConnectSchema>) -> Result<Value> {
        let schema = schema.map(ConnectSchema::resolved); let schema = schema.as_deref();
        if let Some(schema) = schema {
            if !matches!(schema.name.as_deref(), Some("org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time" | "org.apache.kafka.connect.data.Timestamp")) && !matches!(schema.kind, SchemaKind::I64 | SchemaKind::String) { return Err(error("transform-error", "Schema does not identify a timestamp format")); }
        }
        if *value == Value::Null { return Ok(Value::Null); }
        let logical = schema.is_some_and(|schema|matches!(schema.name.as_deref(),Some("org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time" | "org.apache.kafka.connect.data.Timestamp")));
        if let Some(schema) = schema {
            if logical && super::super::data::date_millis(value).is_none()
                || !logical && (matches!(schema.kind,SchemaKind::I64) && !matches!(value,Value::I64(_)) || matches!(schema.kind,SchemaKind::String) && !matches!(value,Value::Utf8(_))) {
                return Err(error("transform-error","Timestamp value does not match its declared representation"));
            }
        }
        let millis = match value {
            Value::I64(value) => match self.precision { Precision::Seconds => value.saturating_mul(1000), Precision::Millis => *value, Precision::Micros => *value / 1000, Precision::Nanos => *value / 1_000_000 },
            Value::Utf8(value) => self.format.as_mut().ok_or_else(|| error("transform-error", "String timestamp input requires format"))?.read(value)?,
            Value::DateDay(value) => i64::from(*value) * 86_400_000,
            Value::Time { value, unit: TimeUnit::Millisecond, .. } | Value::Timestamp { value, unit: TimeUnit::Millisecond, .. } => *value,
            _ => return Err(error("transform-error", "Value is not a supported timestamp representation")),
        };
        Ok(match self.target {
            Target::String => Value::Utf8(self.format.as_mut().ok_or_else(|| error("transform-error", "String timestamp output requires format"))?.format(millis)?),
            Target::Unix => Value::I64(match self.precision { Precision::Seconds => millis / 1000, Precision::Millis => millis, Precision::Micros => millis.saturating_mul(1000), Precision::Nanos => millis.saturating_mul(1_000_000) }),
            Target::Date => {
                let mut fields = super::super::legacy_calendar::DateFields::from_local_millis(millis).raw();
                fields.hour = 0;fields.minute = 0;fields.second = 0;fields.millisecond = 0;
                super::super::data::date_value(fields.local_millis())
            }
            Target::Time => super::super::data::date_value(millis.rem_euclid(86_400_000)),
            Target::Timestamp => super::super::data::date_value(millis),
        })
    }
    fn fields(&mut self, schema: &ConnectSchema, output: &ConnectSchema, value: &Value) -> Result<Value> {
        let schema = schema.resolved();
        if *value == Value::Null { return Ok(Value::Null); }
        let data = ConnectData { schema: Some(Arc::new(schema.as_ref().clone())), value: value.clone() };
        let mut values = fields_with_defaults(&data, self.replace)?;
        let SchemaKind::Struct(fields) = &schema.kind else { return Err(error("transform-error","Field TimestampConverter requires a Struct")); };
        let SchemaKind::Struct(output_fields) = &output.kind else { unreachable!() };
        let mut result = BTreeMap::new();
        for (field, output_field) in fields.iter().zip(output_fields) {
            let value = values.remove(&field.name).unwrap_or(Value::Null);
            let value = if field.name == self.field { self.convert(&value, Some(&field.schema))? } else { value };
            super::super::data::validate_value(&output_field.schema,&value,0)?;
            result.insert(field.name.clone(), value);
        }
        Ok(Value::Struct(result))
    }
}
impl Transform for TimestampConverter {
    fn allocation_bound(&self, record: &SinkRecord) -> Result<usize> {
        record.retained_bytes()?.checked_mul(8).and_then(|bytes|bytes.checked_add(self.format.as_ref().map_or(0,DatePattern::maximum_output_bytes).checked_mul(4)?))
            .ok_or_else(||error("allocation-overflow","Timestamp conversion allocation exceeds range"))
    }
    fn apply(&mut self, mut record: SinkRecord) -> Result<Option<SinkRecord>> {
        let data = if self.key { &mut record.key } else { &mut record.value };
        let mut retained_schema = None;
        super::resolve_record_schema(data);
        if self.field.is_empty() {
            let value = self.convert(&data.value, data.schema.as_deref())?;
            let schema = data.schema.as_ref().map(|schema| self.schema(schema.optional));
            *data = ConnectData { schema, value };
        } else if let Some(schema) = &data.schema {
            if !matches!(data.value, Value::Struct(_) | Value::Null) { return Err(error("transform-error", "Field TimestampConverter requires a Struct or null")); }
            let (updated,charge) = self.updated_schema(schema)?;
            let value = self.fields(schema, &updated, &data.value)?;
            *data = ConnectData { schema: Some(updated), value };
            retained_schema = Some(charge);
        } else if data.value != Value::Null {
            let mut values = fields_with_defaults(data, self.replace)?;
            let field = self.field.clone();
            let value = self.convert(values.get(&field).unwrap_or(&Value::Null), None)?;
            values.insert(field, value);
            data.value = string_map(values);
        }
        if let Some(charge) = retained_schema { record.reservations.push(charge); }
        Ok(Some(record))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn date_pattern(text: &str) -> DatePattern { DatePattern::compile(text,super::super::super::date_environment::test_environment("en","US","UTC"),true).unwrap() }
    #[test]
    fn unix_precision_truncates_toward_zero_and_saturates_like_java_timeunit() {
        let mut converter = TimestampConverter { key: false, field: String::new(), target: Target::Timestamp, precision: Precision::Micros, format: None, replace: true,environment: super::super::super::date_environment::test_environment("en","US","UTC"),schemas: VecDeque::new() };
        assert_eq!(converter.convert(&Value::I64(-1999), None).unwrap(), Value::Timestamp { value: -1, unit: TimeUnit::Millisecond, timezone: Some("UTC".into()) });
        let mut converter = TimestampConverter { precision: Precision::Seconds, ..converter };
        assert_eq!(converter.convert(&Value::I64(i64::MAX), None).unwrap(), Value::Timestamp { value: i64::MAX, unit: TimeUnit::Millisecond, timezone: Some("UTC".into()) });
    }
    #[test]
    fn date_and_time_targets_retain_java_date_milliseconds_at_storage_edges() {
        let mut date = TimestampConverter { key: false,field: String::new(),target: Target::Date,precision: Precision::Millis,format: None,replace: true,environment: super::super::super::date_environment::test_environment("en","US","UTC"),schemas: VecDeque::new() };
        for millis in [i64::MIN,-1,0,1,i64::MAX] {
            let value = Value::I64(millis);
            assert_eq!(date.convert(&value,None).unwrap(),super::super::super::data::date_value(millis.wrapping_sub(millis.rem_euclid(86_400_000))));
        }
        let mut time = TimestampConverter { target: Target::Time,..date };
        assert_eq!(time.convert(&Value::I64(-1),None).unwrap(),super::super::super::data::date_value(86_399_999));
    }
    #[test]
    fn date_pattern_preserves_quotes_width_lenient_day_and_zone() {
        let mut pattern = date_pattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        assert_eq!(pattern.read("1970-01-32T01:00:00.001+01:00").unwrap(), 31 * 86_400_000 + 1);
        assert_eq!(pattern.format(1).unwrap(), "1970-01-01T00:00:00.001Z");
        assert_eq!(date_pattern("yyyyMMdd").format(86_400_000).unwrap(), "19700102");
    }
    #[test]
    fn utc_timestamp_patterns_use_hybrid_calendar_and_full_millisecond_range() {
        let mut pattern = date_pattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
        let julian = super::super::super::legacy_calendar::RawFields { year: 1500,month0: 1,day: 29,..Default::default() }.local_millis();
        assert_eq!(pattern.format(julian).unwrap(),"1500-02-29T00:00:00.000Z");
        assert_eq!(pattern.read("1500-02-29T00:00:00.000Z").unwrap(),julian);
        let text = pattern.format(i64::MAX).unwrap();
        assert_eq!(pattern.read(&text).unwrap(),i64::MAX);
        assert!(pattern.format(i64::MIN).unwrap().ends_with('Z'));
        assert_eq!(date_pattern("yyyy-MM-dd").read("1582-10-10").unwrap(),date_pattern("yyyy-MM-dd").read("1582-10-20").unwrap());
    }
    #[test]
    fn field_schema_default_is_converted_before_values_and_then_reused_with_its_charge() {
        let environment = super::super::super::date_environment::test_environment("en","US","UTC");
        let field = super::super::super::ConnectField { name: "when".into(),schema: Arc::new(ConnectSchema::new(SchemaKind::String)) };
        let mut source = ConnectSchema::new(SchemaKind::Struct(vec![field]));
        source.default = Some(Value::Struct(BTreeMap::from([("when".into(),Value::Utf8("PST 1970".into()))])));
        let source = Arc::new(source);
        let mut converter = TimestampConverter { key: false,field: "when".into(),target: Target::Timestamp,precision: Precision::Millis,
            format: Some(DatePattern::compile("z yyyy",Arc::clone(&environment),true).unwrap()),replace: true,environment,schemas: VecDeque::new() };
        let record = |value: &str| {
            let data = ConnectData { schema: Some(Arc::clone(&source)),value: Value::Struct(BTreeMap::from([("when".into(),Value::Utf8(value.into()))])) };
            SinkRecord::new("topic".into(),0,0,ConnectData::null(),data)
        };
        let first = converter.apply(record("China Standard Time 1970")).unwrap().unwrap();
        let default = first.value.schema.as_ref().unwrap().default.as_ref().unwrap();
        assert_eq!(default,&Value::Struct(BTreeMap::from([("when".into(),super::super::super::data::date_value(8 * 3_600_000))])));
        let cached = converter.updated_schema(&source).unwrap();
        assert!(Arc::ptr_eq(&cached.0,first.value.schema.as_ref().unwrap()));
        assert!(first.reservations.iter().any(|charge|Arc::ptr_eq(charge,&cached.1)));
        assert_eq!(converter.schemas.len(),1);
        converter.target = Target::String;
        assert_eq!(converter.convert(&Value::I64(0),None).unwrap(),Value::Utf8("CST 1970".into()));
        let before = cached.1.budget().used_bytes();drop(converter);
        assert!(cached.1.budget().used_bytes() < before);
        assert!(Arc::strong_count(&cached.1) > 1);
    }
    #[test]
    fn router_constructs_its_formatter_only_after_a_record_supplies_a_timestamp() {
        let environment = super::super::super::date_environment::test_environment("en","US","UTC");
        let mut router = router(&BTreeMap::from([("timestamp.format".into(),"invalid".into())]),environment).unwrap();
        let record = || SinkRecord::new("topic".into(),0,0,ConnectData::null(),ConnectData::null());
        assert!(router.apply(record()).err().unwrap().message.as_deref().is_some_and(|message| message.contains("TimestampRouter requires a record timestamp")));
        let mut record = record();
        record.timestamp_millis = Some(0);
        assert!(router.apply(record).err().unwrap().message.as_deref().is_some_and(|message| message.contains("Invalid date pattern field")));
    }
    #[test]
    fn converter_pattern_blank_check_uses_java_trim_without_unicode_space_folding() {
        let options = |format: &str| BTreeMap::from([("target.type".into(),"string".into()),("format".into(),format.into())]);
        let environment = || super::super::super::date_environment::test_environment("en","US","UTC");
        assert!(converter(&options(" \t\u{0000}"),false,environment()).is_err());
        let mut converter = converter(&options("\u{00a0}"),false,environment()).unwrap();
        let record = SinkRecord::new("topic".into(),0,0,ConnectData::null(),ConnectData::schemaless(Value::I64(0)));
        assert_eq!(converter.apply(record).unwrap().unwrap().value.value,Value::Utf8("\u{00a0}".into()));
    }
}
