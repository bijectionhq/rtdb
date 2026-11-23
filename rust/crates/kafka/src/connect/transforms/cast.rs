use rtdb_types::StructFields;
use std::{collections::{BTreeMap, VecDeque}, sync::Arc};
use base64::Engine;
use rtdb_types::{Result, Value};
use super::{ConnectData, ConnectSchema, SchemaKind, SinkRecord, Transform, error, fields_with_defaults, list, string_map};
use super::super::{DateEnvironment,DatePattern};

#[derive(Clone, Copy)]
enum Target { I8, I16, I32, I64, F32, F64, Bool, String }
impl Target {
    fn parse(value: &str) -> Result<Self> { Ok(match value.trim().to_ascii_lowercase().as_str() {
        "int8" => Self::I8, "int16" => Self::I16, "int32" => Self::I32, "int64" => Self::I64,
        "float32" => Self::F32, "float64" => Self::F64, "boolean" => Self::Bool, "string" => Self::String,
        _ => return Err(error("invalid-plugin-config", "Unsupported Cast output type")),
    }) }
    fn kind(self) -> SchemaKind { match self { Self::I8 => SchemaKind::I8, Self::I16 => SchemaKind::I16, Self::I32 => SchemaKind::I32, Self::I64 => SchemaKind::I64, Self::F32 => SchemaKind::F32, Self::F64 => SchemaKind::F64, Self::Bool => SchemaKind::Bool, Self::String => SchemaKind::String } }
}
struct Cast { key: bool, whole: Option<Target>, fields: BTreeMap<String, Target>, replace: bool, environment: Arc<DateEnvironment>, schemas: VecDeque<CachedSchema> }
struct CachedSchema { source: Arc<ConnectSchema>, output: Arc<ConnectSchema>, charge: Arc<rtdb_columnar::MemoryReservation> }
pub(super) fn open(options: &BTreeMap<String, String>, key: bool, environment: Arc<DateEnvironment>) -> Result<Box<dyn Transform>> {
    let specifications = list(options, "spec");
    if specifications.is_empty() { return Err(error("invalid-plugin-config", "Cast requires a nonempty spec")); }
    let mut whole = None; let mut fields = BTreeMap::new();
    for specification in &specifications {
        if let Some((name, kind)) = specification.split_once(':') { fields.insert(name.trim().into(), Target::parse(kind)?); }
        else { whole = Some(Target::parse(specification)?); }
    }
    if whole.is_some() && specifications.len() != 1 { return Err(error("invalid-plugin-config", "Whole-value Cast requires exactly one type")); }
    Ok(Box::new(Cast { key, whole, fields, replace: super::super::converter::option_bool(options, "replace.null.with.default", true)?, environment, schemas: VecDeque::new() }))
}
impl Cast {
    fn updated_schema(&mut self, source: &Arc<ConnectSchema>) -> Result<(Arc<ConnectSchema>, Arc<rtdb_columnar::MemoryReservation>)> {
        if let Some(index) = self.schemas.iter().position(|cached| cached.source == *source) {
            let cached = self.schemas.remove(index).unwrap();
            let result = (Arc::clone(&cached.output), Arc::clone(&cached.charge));
            self.schemas.push_back(cached);
            return Ok(result);
        }
        let bound = source.retained_bytes()?.checked_mul(8).and_then(|size| size.checked_add(self.fields.len().max(1).checked_mul(256 << 10)?))
            .ok_or_else(|| error("allocation-overflow", "Cast schema cache allocation exceeds range"))?;
        let charge = self.environment.reserve(bound)?;
        let output = if let Some(target) = self.whole {
            updated_schema(source, target, true, &self.environment)?
        } else {
            let mut updated = source.as_ref().clone();
            let SchemaKind::Struct(fields) = &mut updated.kind else { return Err(error("transform-error", "Field Cast requires a Struct")); };
            for field in fields {
                if let Some(target) = self.fields.get(&field.name) { field.schema = updated_schema(&field.schema, *target, false, &self.environment)?; }
            }
            if source.default_value().is_some() { return Err(error("transform-error", "Cast cannot convert a top-level Struct default")); }
            updated.default = None;
            Arc::new(updated)
        };
        if self.schemas.len() == 16 { self.schemas.pop_front(); }
        self.schemas.push_back(CachedSchema { source: Arc::clone(source), output: Arc::clone(&output), charge: Arc::clone(&charge) });
        Ok((output, charge))
    }
}
impl Transform for Cast {
    fn allocation_bound(&self, record: &SinkRecord) -> Result<usize> {
        record.retained_bytes()?.checked_mul(8).and_then(|size|size.checked_add(self.fields.len().max(1).checked_mul(256 << 10)?))
            .ok_or_else(||error("allocation-overflow","Cast allocation exceeds range"))
    }
    fn apply(&mut self, mut record: SinkRecord) -> Result<Option<SinkRecord>> {
        let data = if self.key { &mut record.key } else { &mut record.value };
        super::resolve_record_schema(data);
        if data.value == Value::Null { return Ok(Some(record)); }
        let cached = data.schema.as_ref().map(|schema| self.updated_schema(schema)).transpose()?;
        if let Some(target) = self.whole {
            let value = cast_with_schema(&data.value,data.schema.as_deref(),target,&self.environment)?;
            let schema = cached.as_ref().map(|(schema, _)| Arc::clone(schema));
            *data = ConnectData { schema, value };
        } else {
            if data.schema.is_some() && !matches!(data.value, Value::Struct(_)) { return Err(error("transform-error", "Field Cast requires a Struct")); }
            let mut values = fields_with_defaults(data, self.replace)?;
            let schema = if let Some(schema) = &data.schema {
                let SchemaKind::Struct(fields) = &schema.kind else { return Err(error("transform-error", "Field Cast requires a Struct")); };
                let Some((output, _)) = &cached else { unreachable!() };
                let SchemaKind::Struct(output_fields) = &output.kind else { unreachable!() };
                let mut result = StructFields::new();
                for (field, output_field) in fields.iter().zip(output_fields) {
                    let value = values.remove(&field.name).unwrap_or(Value::Null);
                    let value = match self.fields.get(&field.name) { Some(target) => cast_with_schema(&value, Some(&field.schema), *target, &self.environment)?, None => value };
                    super::super::data::validate_value(&output_field.schema, &value, 0)?;
                    result.insert(field.name.clone(), value);
                }
                values = result;
                cached.as_ref().map(|(schema, _)| Arc::clone(schema))
            } else {
                for (name, target) in &self.fields { values.insert(name.clone(), cast_with_schema(values.get(name).unwrap_or(&Value::Null),None,*target,&self.environment)?); }
                None
            };
            *data = ConnectData { value: if schema.is_some() { Value::Struct(values) } else { string_map(values) }, schema };
        }
        if let Some((_, charge)) = cached { record.reservations.push(charge); }
        Ok(Some(record))
    }
}
fn updated_schema(schema: &ConnectSchema, target: Target, basics: bool, environment: &Arc<DateEnvironment>) -> Result<Arc<ConnectSchema>> {
    let mut result = if basics { schema.clone() } else { ConnectSchema::new(target.kind()) };
    result.kind = target.kind(); result.optional = schema.optional;
    result.default = schema.default_value().map(|value| cast_with_schema(value,Some(schema),target,environment)).transpose()?;
    if let Some(default) = &result.default { super::super::data::validate_value(&result, default, 0)?; }
    Ok(Arc::new(result))
}
fn integer(value: &Value, long: bool) -> Result<i64> {
    Ok(match value {
        Value::Bool(value) => i64::from(*value), Value::I8(value) => i64::from(*value), Value::I16(value) => i64::from(*value), Value::I32(value) => i64::from(*value), Value::I64(value) => *value,
        Value::F32(value) => if long { *value as i64 } else { i64::from(*value as i32) }, Value::F64(value) => if long { *value as i64 } else { i64::from(*value as i32) },
        Value::DateDay(value) => i64::from(*value), Value::Time { value, unit: rtdb_types::TimeUnit::Millisecond, .. } | Value::Timestamp { value, unit: rtdb_types::TimeUnit::Millisecond, .. } => *value,
        _ => return Err(error("transform-error", "Value cannot be cast to an integer")),
    })
}
fn floating(value: &Value) -> Result<f64> {
    match value { Value::F32(value) => Ok(f64::from(*value)), Value::F64(value) => Ok(*value), Value::Utf8(value) => value.trim().parse::<f64>().map_err(|cause| error("transform-error", cause)), _ => Ok(integer(value, true)? as f64) }
}
fn cast_with_schema(value: &Value,schema: Option<&ConnectSchema>,target: Target,environment: &Arc<DateEnvironment>) -> Result<Value> {
    if *value == Value::Null { return Ok(Value::Null); }
    if schema.is_none() && super::super::data::date_millis(value).is_some() {
        return Err(error("transform-error","Schemaless java.util.Date has no Connect primitive type"));
    }
    if matches!(target,Target::String) {
        if let Some(millis) = super::super::data::date_millis(value) { return date_string(millis,environment).map(Value::Utf8); }
    }
    if !matches!(target,Target::String) {
        if let Some(schema) = schema {
            if let Some(encoded) = super::super::data::encode_logical_date(schema,value)? { return cast(&encoded,target); }
        }
    }
    cast(value,target)
}
fn cast(value: &Value, target: Target) -> Result<Value> {
    if *value == Value::Null { return Ok(Value::Null); }
    macro_rules! integral { ($kind:ident, $ty:ty, $long:expr) => { Value::$kind(match value { Value::Utf8(value) => value.parse::<$ty>().map_err(|cause| error("transform-error", cause))?, _ => integer(value, $long)? as $ty }) }; }
    Ok(match target {
        Target::I8 => integral!(I8, i8, false), Target::I16 => integral!(I16, i16, false), Target::I32 => integral!(I32, i32, false), Target::I64 => integral!(I64, i64, true),
        Target::F32 => Value::F32(floating(value)? as f32), Target::F64 => Value::F64(floating(value)?),
        Target::Bool => Value::Bool(match value { Value::Utf8(value) => value.eq_ignore_ascii_case("true"), _ => integer(value, true)? != 0 }),
        Target::String => Value::Utf8(match value {
            Value::Utf8(value) => value.clone(), Value::Bool(value) => value.to_string(), Value::I8(value) => value.to_string(), Value::I16(value) => value.to_string(), Value::I32(value) => value.to_string(), Value::I64(value) => value.to_string(),
            Value::F32(value) => float_string(f64::from(*value), value.to_string()), Value::F64(value) => float_string(*value, value.to_string()),
            Value::Binary(value) => base64::engine::general_purpose::STANDARD.encode(value),
            _ => return Err(error("transform-error", "Value cannot be cast to a string")),
        }),
    })
}
fn float_string(value: f64, text: String) -> String {
    if value.is_nan() { "NaN".into() } else if value == f64::INFINITY { "Infinity".into() } else if value == f64::NEG_INFINITY { "-Infinity".into() }
    else if !text.contains(['.', 'e', 'E']) { format!("{text}.0") } else { text }
}
fn date_string(millis: i64, environment: &Arc<DateEnvironment>) -> Result<String> {
    DatePattern::compile(if millis < 86_400_000 { "HH:mm:ss.SSS'Z'" } else if millis % 86_400_000 == 0 { "yyyy-MM-dd" } else { "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" },Arc::clone(environment),false)?.format(millis)
}
pub(super) fn mask(value: Value, replacement: Option<&str>) -> Result<Value> {
    if value == Value::Null { return Ok(value); }
    if let Some(replacement) = replacement {
        if matches!(value, Value::Utf8(_)) { return Ok(Value::Utf8(replacement.into())); }
        use std::str::FromStr;
        let spelling = rtdb_types::decimal_spelling(replacement).map_err(|cause| error("transform-error", cause))?;
        let decimal = bigdecimal::BigDecimal::from_str(spelling.ascii()).map_err(|cause| error("transform-error", cause))?;
        let (coefficient, scale) = decimal.as_bigint_and_exponent();
        if matches!(value, Value::Decimal(_)) {
            let digits = coefficient.to_str_radix(10).trim_start_matches('-').len(); let precision = if digits <= 32 { 32u8 } else if digits <= 64 { 64 } else { return Err(error("transform-error", "Decimal mask exceeds the document writer width")); };
            let bytes = coefficient.to_signed_bytes_be(); let mut unscaled = vec![if coefficient.sign() == num_bigint::Sign::Minus { 255 } else { 0 }; usize::from(precision / 2)];
            if bytes.len() > unscaled.len() { return Err(error("transform-error", "Decimal mask exceeds its coefficient width")); }
            let start = unscaled.len() - bytes.len(); unscaled[start..].copy_from_slice(&bytes);
            return Ok(Value::Decimal(rtdb_types::DecimalValue::new(unscaled, i32::from(precision), i32::try_from(scale).map_err(|cause| error("transform-error", cause))?, u16::from(precision) * 4)?));
        }
        let target = match value { Value::I8(_) => Target::I8, Value::I16(_) => Target::I16, Value::I32(_) => Target::I32, Value::I64(_) => Target::I64, Value::F32(_) => Target::F32, Value::F64(_) => Target::F64, Value::Utf8(_) => Target::String, _ => return Err(error("transform-error", "Custom mask requires numeric or string input")) };
        if matches!(target, Target::F32 | Target::F64) {
            return if coefficient.sign() == num_bigint::Sign::NoSign { Ok(if matches!(target, Target::F32) { Value::F32(0.0) } else { Value::F64(0.0) }) }
                else { cast(&Value::Utf8(spelling.ascii().into()), target) };
        }
        let coefficient = if scale > 0 {
            if scale as u64 > coefficient.to_str_radix(10).len() as u64 { num_bigint::BigInt::from(0) }
            else { coefficient / num_bigint::BigInt::from(10).pow(scale as u32) }
        } else { coefficient };
        let bytes = coefficient.to_signed_bytes_le(); let mut low = [if coefficient.sign() == num_bigint::Sign::Minus { 255 } else { 0 }; 8];
        let count = bytes.len().min(8); low[..count].copy_from_slice(&bytes[..count]);
        let integer = if scale < 0 { if scale.unsigned_abs() >= 64 { 0 } else { i64::from_le_bytes(low).wrapping_mul(10i64.wrapping_pow((-scale) as u32)) } } else { i64::from_le_bytes(low) };
        return cast(&Value::I64(integer), target);
    }
    Ok(match value {
        Value::Bool(_) => Value::Bool(false), Value::I8(_) => Value::I8(0), Value::I16(_) => Value::I16(0), Value::I32(_) => Value::I32(0), Value::I64(_) => Value::I64(0), Value::F32(_) => Value::F32(0.0), Value::F64(_) => Value::F64(0.0), Value::Utf8(_) => Value::Utf8(String::new()),
        Value::DateDay(_) => Value::DateDay(0), Value::Time { unit, bit_width, .. } => Value::Time { value: 0, unit, bit_width }, Value::Timestamp { unit, timezone, .. } => Value::Timestamp { value: 0, unit, timezone },
        Value::Decimal(_) => Value::Decimal(rtdb_types::DecimalValue::new(vec![0;16], 32, 0, 128)?),
        Value::List(_) => Value::list(Vec::new()), Value::Map { .. } => Value::Map { entries: Vec::new(), sorted: false },
        _ => return Err(error("transform-error", "Input type has no Connect null mask")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn cast_with_schema(value: &Value,schema: Option<&ConnectSchema>,target: Target) -> Result<Value> {
        super::cast_with_schema(value,schema,target,&super::super::super::date_environment::test_environment("en","US","UTC"))
    }
    #[test]
    fn primitive_cast_uses_schema_logical_encoding_without_losing_raw_date_milliseconds() {
        let mut schema = ConnectSchema::new(SchemaKind::I32);
        schema.name = Some("org.apache.kafka.connect.data.Date".into());
        let day = i64::from(i32::MAX) + 1;
        let value = super::super::super::data::date_value(day * 86_400_000);
        assert_eq!(cast_with_schema(&value,Some(&schema),Target::I64).unwrap(),Value::I64(i64::from(i32::MIN)));
        assert!(cast_with_schema(&super::super::super::data::date_value(1),Some(&schema),Target::I64).is_err());
        schema.name = Some("org.apache.kafka.connect.data.Time".into());
        assert_eq!(cast_with_schema(&super::super::super::data::date_value(86_400_000),Some(&schema),Target::I64).unwrap(),Value::I64(86_400_000));
        assert!(cast_with_schema(&super::super::super::data::date_value(86_400_001),Some(&schema),Target::I64).is_err());
        assert!(cast_with_schema(&value,None,Target::I64).is_err());
    }
    #[test]
    fn date_string_chooses_pattern_from_raw_millis_then_formats_in_the_process_zone() {
        let environment = super::super::super::date_environment::test_environment("en","US","GMT+01:00");
        assert_eq!(date_string(-1,&environment).unwrap(),"00:59:59.999Z");
        assert_eq!(date_string(86_400_000,&environment).unwrap(),"1970-01-02");
        assert_eq!(date_string(86_400_001,&environment).unwrap(),"1970-01-02T01:00:00.001Z");
        assert!(!date_string(i64::MAX,&environment).unwrap().is_empty());
    }
    #[test]
    fn custom_numeric_masks_use_bigdecimal_truncation_and_wrapping() {
        assert_eq!(mask(Value::I8(1), Some("256.9")).unwrap(), Value::I8(0));
        assert_eq!(mask(Value::I64(1), Some("18446744073709551617")).unwrap(), Value::I64(1));
        assert_eq!(mask(Value::Bool(true), Some("true")).unwrap_err().code, "xtdb.kafka-connect-source/transform-error");
    }
    #[test]
    fn decimal_masks_keep_full_scale_and_integer_masks_bound_large_powers() {
        for (text, scale) in [("1E+129", -129), ("1E-129", 129), ("1E+2147483648", i32::MIN), ("1E-2147483647", i32::MAX)] {
            let source = Value::Decimal(rtdb_types::DecimalValue::new(vec![0; 16], 32, 0, 128).unwrap());
            let Value::Decimal(value) = mask(source, Some(text)).unwrap() else { panic!("expected Decimal") };
            assert_eq!(value.scale, scale);
            assert_eq!(num_bigint::BigInt::from_signed_bytes_be(&value.unscaled), 1.into());
            assert_eq!(mask(Value::I64(5), Some(text)).unwrap(), Value::I64(0));
        }
        assert!(mask(Value::I64(1), Some("1_0")).is_err());
        assert!(mask(Value::I64(1), Some("1E-2147483648")).is_err());
        assert_eq!(mask(Value::I64(1), Some("１２８.９")).unwrap(), Value::I64(128));
        assert_eq!(mask(Value::F32(1.0), Some("١.٢٥")).unwrap(), Value::F32(1.25));
        let Value::F64(zero) = mask(Value::F64(1.0), Some("-0.0")).unwrap() else { panic!("expected F64") };
        assert_eq!(zero.to_bits(), 0.0f64.to_bits());
    }
}
