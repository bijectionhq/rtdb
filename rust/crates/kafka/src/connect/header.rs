use std::{str::FromStr, sync::Arc};
use rtdb_types::{DecimalValue, Result, Value};
use super::{ConnectData, ConnectSchema, SchemaKind, error};

pub(super) fn decode_with_environment(bytes: Option<&[u8]>, environment: &Arc<super::DateEnvironment>) -> Result<ConnectData> {
    let Some(bytes) = bytes else { return Ok(ConnectData::null()); };
    match literal_with_environment(&String::from_utf8_lossy(bytes), environment) {
        Ok(value) => Ok(value),
        Err(cause) if cause.category == rtdb_types::AnomalyCategory::Incorrect => Ok(typed(SchemaKind::Bytes, Value::Binary(bytes.to_vec()))),
        Err(cause) => Err(cause),
    }
}
pub(super) fn literal_with_environment(text: &str, environment: &Arc<super::DateEnvironment>) -> Result<ConnectData> {
    if text.is_empty() { return Ok(ConnectData::null()); }
    let mut parser = Parser { original: text, tokens: tokens(text), next: 0, environment };
    parser.value(false, 0)
}
fn typed(kind: SchemaKind, value: Value) -> ConnectData { ConnectData { schema: Some(Arc::new(ConnectSchema::new(kind))), value } }
fn tokens(text: &str) -> Vec<&str> {
    let mut result = Vec::new(); let mut start = 0; let mut escaped = false;
    for (index, character) in text.char_indices() {
        if character == '\\' { escaped = !escaped; continue; }
        if !escaped && matches!(character, ':' | ',' | '{' | '}' | '[' | ']' | '"') {
            if start < index { result.push(&text[start..index]); }
            result.push(&text[index..index + 1]); start = index + 1;
        }
        escaped = false;
    }
    if start < text.len() { result.push(&text[start..]); } result
}
struct Parser<'a> { original: &'a str, tokens: Vec<&'a str>, next: usize, environment: &'a Arc<super::DateEnvironment> }
impl Parser<'_> {
    fn take(&mut self) -> Result<&str> { let value = self.tokens.get(self.next).copied().ok_or_else(|| error("header-parse", "Unexpected end of header literal"))?; self.next += 1; Ok(value) }
    fn can(&mut self, expected: &str) -> bool {
        while self.next + 1 < self.tokens.len() && self.tokens[self.next].trim().is_empty() { self.next += 1; }
        if self.tokens.get(self.next).is_some_and(|token| token.trim() == expected) { self.next += 1; true } else { false }
    }
    fn value(&mut self, embedded: bool, depth: usize) -> Result<ConnectData> {
        if depth > 128 { return Err(error("header-parse", "Header literal nesting limit exceeded")); }
        if self.next == self.tokens.len() { return Ok(ConnectData::null()); }
        if embedded && self.can("\"") {
            let mut content = String::new();
            while self.next < self.tokens.len() { if self.can("\"") { break; } content.push_str(self.take()?); }
            return Ok(temporal(&content,self.environment)?.unwrap_or_else(|| typed(SchemaKind::String, Value::Utf8(content))));
        }
        for (token, value) in [("null", None), ("true", Some(true)), ("false", Some(false))] {
            let mark = self.next;
            if self.can(token) && (embedded || self.next == self.tokens.len()) { return Ok(value.map_or_else(ConnectData::null, |value| typed(SchemaKind::Bool, Value::Bool(value)))); }
            self.next = mark;
        }
        let mark = self.next;
        let collection = if self.can("[") { Some(self.array(depth + 1)) } else if self.can("{") { Some(self.map(depth + 1)) } else { None };
        match collection {
            Some(Ok(value)) => return Ok(value),
            Some(Err(cause)) if cause.category != rtdb_types::AnomalyCategory::Incorrect => return Err(cause),
            _ => (),
        }
        self.next = mark;
        let token = self.take()?.to_owned();
        if token.trim().is_empty() { return Ok(typed(SchemaKind::String, Value::Utf8(token))); }
        let token = token.trim();
        if token.starts_with(|character: char| rtdb_types::decimal_digit(character).is_some()) {
            if self.next + 4 <= self.tokens.len() {
                let extended = format!("{token}{}", self.tokens[self.next..self.next + 4].concat());
                if let Some(value) = temporal(&extended,self.environment)? { self.next += 4; return Ok(value); }
            }
            if let Some(value) = temporal(token,self.environment)? { return Ok(value); }
        }
        if token.starts_with(|character: char| rtdb_types::decimal_digit(character).is_some() || character == '+' || character == '-') { if let Ok(value) = number(token) { return Ok(value); } }
        if embedded { Err(error("header-parse", "Invalid embedded header literal")) } else { Ok(typed(SchemaKind::String, Value::Utf8(self.original.into()))) }
    }
    fn array(&mut self, depth: usize) -> Result<ConnectData> {
        let mut values = Vec::new(); let mut schema = Merger::default();
        loop {
            if self.can("]") {
                let kind = match schema.common { Some(element) => { for value in &mut values { *value = align(std::mem::replace(value, Value::Null), &element)?; } SchemaKind::Array(element) }, None => SchemaKind::ArrayOfNull };
                return Ok(typed(kind, Value::list(values)));
            }
            if self.next == self.tokens.len() || self.can(",") { return Err(error("header-parse", "Missing array element or closing bracket")); }
            let value = self.value(true, depth)?; schema.merge(value.schema); values.push(value.value);
            let mark = self.next;
            if self.can("]") { self.next = mark; } else if !self.can(",") { return Err(error("header-parse", "Missing array comma")); }
        }
    }
    fn map(&mut self, depth: usize) -> Result<ConnectData> {
        let mut entries = Vec::new(); let mut keys = Merger::default(); let mut values = Merger::default();
        loop {
            if self.can("}") {
                let kind = match (keys.common, values.common) {
                    (Some(key), Some(value)) => { for (k, v) in &mut entries { *k = align(std::mem::replace(k, Value::Null), &key)?; *v = align(std::mem::replace(v, Value::Null), &value)?; } SchemaKind::Map(key, value) }
                    (Some(key), None) => { for (k, _) in &mut entries { *k = align(std::mem::replace(k, Value::Null), &key)?; } SchemaKind::MapWithNullValues(key) }
                    _ => SchemaKind::MapOfNull,
                };
                return Ok(typed(kind, Value::Map { entries, sorted: false }));
            }
            if self.next == self.tokens.len() || self.can(",") { return Err(error("header-parse", "Missing map entry or closing brace")); }
            let key = self.value(true, depth)?;
            if key.value == Value::Null || !self.can(":") { return Err(error("header-parse", "Map key is null or lacks a colon")); }
            let value = self.value(true, depth)?;
            if let Some((_, old)) = entries.iter_mut().find(|(old, _)| *old == key.value) { *old = value.value; } else { entries.push((key.value, value.value)); }
            keys.merge(key.schema); values.merge(value.schema); self.can(",");
        }
    }
}
#[derive(Default)]
struct Merger { common: Option<Arc<ConnectSchema>>, incompatible: bool }
impl Merger {
    fn merge(&mut self, next: Option<Arc<ConnectSchema>>) {
        if self.incompatible { return; }
        let Some(next) = next else { return; }; let Some(previous) = &self.common else { self.common = Some(next); return; };
        if std::mem::discriminant(&previous.kind) == std::mem::discriminant(&next.kind) {
            if previous.optional == next.optional { if !previous.optional { self.common = Some(next); } }
            else if previous != &next { self.common = None; self.incompatible = true; }
        } else if let (Some(a), Some(b)) = (rank(&previous.kind), rank(&next.kind)) { if b > a { self.common = Some(next); } }
        else { self.common = None; self.incompatible = true; }
    }
}
fn rank(kind: &SchemaKind) -> Option<u8> { match kind { SchemaKind::I8 => Some(0), SchemaKind::I16 => Some(1), SchemaKind::I32 => Some(2), SchemaKind::I64 => Some(3), SchemaKind::F32 => Some(4), SchemaKind::F64 => Some(5), _ => None } }
fn align(value: Value, schema: &ConnectSchema) -> Result<Value> {
    if value == Value::Null { return Ok(value); }
    let logical = schema.name.as_deref().filter(|name| matches!(*name, "org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time" | "org.apache.kafka.connect.data.Timestamp"));
    if super::data::date_millis(&value).is_some() {
        if logical.is_some() { return Ok(value); }
        if rank(&schema.kind).is_some() { return Err(error("header-parse", "A date object has no numeric schema during header alignment")); }
    }
    if schema.name.is_some() && logical.is_none() { return Ok(value); }
    let numeric = match &value { Value::I8(v) => Some((i64::from(*v), f64::from(*v))), Value::I16(v) => Some((i64::from(*v), f64::from(*v))), Value::I32(v) => Some((i64::from(*v), f64::from(*v))), Value::I64(v) => Some((*v, *v as f64)), Value::F32(v) => Some((*v as i64, f64::from(*v))), Value::F64(v) => Some((*v as i64, *v)), _ => None };
    if let Some((integer, float)) = numeric {
        if let Some(logical) = logical {
            let millis = match logical {
                "org.apache.kafka.connect.data.Date" => i64::from(integer as i32) * 86_400_000,
                "org.apache.kafka.connect.data.Time" => {
                    let millis = integer as i32;
                    if !(0..=86_400_000).contains(&millis) { return Err(error("header-parse", "Aligned Time value is outside one day")); }
                    i64::from(millis)
                }
                _ => integer,
            };
            return Ok(super::data::date_value(millis));
        }
        return Ok(match schema.kind { SchemaKind::I8 => Value::I8(integer as i8), SchemaKind::I16 => Value::I16(integer as i16), SchemaKind::I32 => Value::I32(integer as i32), SchemaKind::I64 => Value::I64(integer), SchemaKind::F32 => Value::F32(float as f32), SchemaKind::F64 => Value::F64(float), _ => value });
    }
    Ok(value)
}
fn number(text: &str) -> Result<ConnectData> {
    let spelling = rtdb_types::decimal_spelling(text).map_err(|cause| error("header-parse", cause))?;
    let text = spelling.ascii();
    let decimal = bigdecimal::BigDecimal::from_str(text).map_err(|cause| error("header-parse", cause))?;
    let (coefficient, scale) = decimal.as_bigint_and_exponent();
    let scale = i32::try_from(scale).map_err(|cause| error("header-parse", cause))?;
    if let Some(integer) = exact_integer(&coefficient, scale) {
        return Ok(if let Ok(value) = i8::try_from(integer) { typed(SchemaKind::I8, Value::I8(value)) } else if let Ok(value) = i16::try_from(integer) { typed(SchemaKind::I16, Value::I16(value)) } else if let Ok(value) = i32::try_from(integer) { typed(SchemaKind::I32, Value::I32(value)) } else { typed(SchemaKind::I64, Value::I64(integer)) });
    }
    if scale != 0 {
        if let Ok(value) = text.parse::<f32>() { if value.is_finite() { return Ok(typed(SchemaKind::F32, Value::F32(value))); } }
        if let Ok(value) = text.parse::<f64>() { if value.is_finite() { return Ok(typed(SchemaKind::F64, Value::F64(value))); } }
    }
    let bytes = coefficient.to_signed_bytes_be(); let digits = coefficient.to_str_radix(10).trim_start_matches('-').len();
    let precision = if digits <= 32 { 32u8 } else if digits <= 64 { 64 } else { return Err(error("header-parse", "Decimal exceeds the reference document width")); };
    let mut unscaled = vec![if coefficient.sign() == num_bigint::Sign::Minus { 255 } else { 0 }; usize::from(precision / 2)];
    if bytes.len() > unscaled.len() { return Err(error("header-parse", "Decimal coefficient exceeds its width")); }
    let start = unscaled.len() - bytes.len(); unscaled[start..].copy_from_slice(&bytes);
    let value = Value::Decimal(DecimalValue::new(unscaled, i32::from(precision), scale, u16::from(precision) * 4)?);
    let mut schema = ConnectSchema::new(SchemaKind::Bytes); schema.name = Some("org.apache.kafka.connect.data.Decimal".into()); schema.version = Some(1); schema.parameters.insert("scale".into(), scale.to_string());
    Ok(ConnectData { schema: Some(Arc::new(schema)), value })
}
fn exact_integer(coefficient: &num_bigint::BigInt, scale: i32) -> Option<i64> {
    if coefficient.sign() == num_bigint::Sign::NoSign { return Some(0); }
    let signed = coefficient.to_str_radix(10);
    let digits = signed.trim_start_matches('-');
    let integer_digits = i64::try_from(digits.len()).ok()?.checked_sub(i64::from(scale))?;
    if !(1..=19).contains(&integer_digits) { return None; }
    if scale >= 0 {
        let end = signed.len().checked_sub(scale as usize)?;
        if signed.as_bytes()[end..].iter().any(|byte| *byte != b'0') { return None; }
        signed[..end].parse().ok()
    } else {
        let mut integer = signed;
        integer.extend(std::iter::repeat_n('0', scale.unsigned_abs() as usize));
        integer.parse().ok()
    }
}
fn temporal(text: &str, environment: &Arc<super::DateEnvironment>) -> Result<Option<ConnectData>> {
    let text = text.replace("\\:", ":");
    let (kind, name, pattern) = match text.encode_utf16().count() {
        10 => (SchemaKind::I32, "Date", "yyyy-MM-dd"),
        13 => (SchemaKind::I32, "Time", "HH:mm:ss.SSS'Z'"),
        24 => (SchemaKind::I64, "Timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        _ => return Ok(None),
    };
    let mut pattern = super::DatePattern::compile(pattern,Arc::clone(environment),false)?;
    let millis = match pattern.read(&text) {
        Ok(value) => value,
        Err(cause) if cause.category == rtdb_types::AnomalyCategory::Incorrect => return Ok(None),
        Err(cause) => return Err(cause),
    };
    let value = super::data::date_value(millis);
    let mut schema = ConnectSchema::new(kind); schema.name = Some(format!("org.apache.kafka.connect.data.{name}")); schema.version = Some(1);
    Ok(Some(ConnectData { schema: Some(Arc::new(schema)), value }))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn literal(text: &str) -> Result<ConnectData> { literal_with_environment(text,&super::super::date_environment::test_environment("en","US","UTC")) }
    #[test]
    fn header_literals_preserve_top_level_quotes_and_narrow_numeric_types() {
        assert_eq!(literal("\"text\"").unwrap().value, Value::Utf8("\"text\"".into()));
        assert_eq!(literal("true}").unwrap().value, Value::Utf8("true}".into()));
        assert_eq!(literal("127").unwrap().value, Value::I8(127)); assert_eq!(literal("128").unwrap().value, Value::I16(128));
        assert_eq!(literal("1.0").unwrap().value, Value::I8(1)); assert_eq!(literal("1.25").unwrap().value, Value::F32(1.25));
    }
    #[test]
    fn header_numeric_inference_handles_full_scales_without_expanding_powers() {
        let value = literal("1E+2147483648").unwrap();
        assert_eq!(value.schema.as_ref().unwrap().parameters["scale"], i32::MIN.to_string());
        let Value::Decimal(value) = value.value else { panic!("expected Decimal") };
        assert_eq!(value.scale, i32::MIN);
        assert_eq!(num_bigint::BigInt::from_signed_bytes_be(&value.unscaled), 1.into());
        assert_eq!(literal("1E-2147483647").unwrap().value, Value::F32(0.0));
        assert_eq!(literal("0E+2147483648").unwrap().value, Value::I8(0));
        assert_eq!(literal("9223372036854775807.0").unwrap().value, Value::I64(i64::MAX));
        assert_eq!(literal("-9223372036854775808.0").unwrap().value, Value::I64(i64::MIN));
        assert!(matches!(literal("9223372036854775808").unwrap().value, Value::Decimal(_)));
        for text in ["1E-2147483648", "1_0"] { assert_eq!(literal(text).unwrap().value, Value::Utf8(text.into())); }
        assert_eq!(literal("１２７").unwrap().value, Value::I8(127));
        assert_eq!(literal("١.٢٥").unwrap().value, Value::F32(1.25));
        assert_eq!(literal("١E-٢١٤٧٤٨٣٦٤٧").unwrap().value, Value::F32(0.0));
        assert_eq!(literal("𝟙").unwrap().value, Value::Utf8("𝟙".into()));
    }
    #[test]
    fn header_collections_align_numbers_and_preserve_mixed_schema_absence() {
        assert_eq!(literal("[1,128]").unwrap().value, Value::list(vec![Value::I16(1), Value::I16(128)]));
        assert!(matches!(literal("[1,\"a\"]").unwrap().schema.unwrap().kind, SchemaKind::ArrayOfNull));
        assert!(matches!(literal("{1:null}").unwrap().schema.unwrap().kind, SchemaKind::MapWithNullValues(_)));
        assert_eq!(literal("[unquoted]").unwrap().value, Value::Utf8("[unquoted]".into()));
    }
    #[test]
    fn header_temporal_literals_preserve_logical_names_and_millisecond_values() {
        assert_eq!(literal("12:34:56.789Z").unwrap().value, super::super::data::date_value(45_296_789));
        assert_eq!(literal("1970-01-02").unwrap().value, super::super::data::date_value(86_400_000));
        assert_eq!(decode_with_environment(None,&super::super::date_environment::test_environment("en","US","UTC")).unwrap(), ConnectData::null());
    }
    #[test]
    fn header_alignment_preserves_raw_dates_and_rejects_untyped_numeric_conversion() {
        let date = literal("1970-01-02").unwrap();
        let schema = date.schema.as_deref().unwrap();
        assert_eq!(align(Value::I8(2), schema).unwrap(), super::super::data::date_value(172_800_000));
        assert_eq!(align(super::super::data::date_value(-1), schema).unwrap(), super::super::data::date_value(-1));
        assert!(align(date.value, &ConnectSchema::new(SchemaKind::I32)).is_err());
        assert_eq!(literal("[1970-01-02,32768]").unwrap().value, Value::Utf8("[1970-01-02,32768]".into()));
    }
    #[test]
    fn literal_z_suffix_keeps_default_zone_and_hybrid_lenient_calendar() {
        let environment = super::super::date_environment::test_environment("en","US","GMT+01:00");
        assert_eq!(literal_with_environment("1970-01-02",&environment).unwrap().value,super::super::data::date_value(23 * 3_600_000));
        assert_eq!(literal_with_environment("00:00:00.000Z",&environment).unwrap().value,super::super::data::date_value(-3_600_000));
        assert_eq!(literal_with_environment("1970-01-32",&environment).unwrap().value,super::super::data::date_value(31 * 86_400_000 - 3_600_000));
        let julian = super::super::legacy_calendar::RawFields { year: 1500,month0: 1,day: 29,..Default::default() }.local_millis();
        assert_eq!(literal_with_environment("1500-02-29",&environment).unwrap().value,super::super::data::date_value(julian - 3_600_000));
    }
}
