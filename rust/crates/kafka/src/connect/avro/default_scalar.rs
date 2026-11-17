use std::mem::size_of;
use apache_avro::Schema;
use rtdb_types::{Anomaly, DecimalValue, Result, Value};
use super::{default_input::{DefaultAttempt, FieldObject, JsonNode, ObjectKind}, default_json::DefaultJson, error};
use super::super::{json_decimal::JsonDecimal, schema_graph::{Scalar, SchemaView, ViewKind}};

#[derive(Clone, Copy)]
pub(in crate::connect::avro) enum ScalarInput<'a> { FieldObject(FieldObject<'a>), JsonNode(JsonNode<'a>), Native(&'a Value) }
#[derive(Clone, Copy, Debug)]
pub(in crate::connect::avro) enum ConversionPath { Wrapper, MapChild }

#[derive(Debug)]
pub(in crate::connect::avro) struct PreparedScalar<'a> { value: PreparedValue<'a> }
#[derive(Debug)]
enum PreparedValue<'a> {
    Missing, Null, Bool(bool), I8(i8), I16(i16), I32(i32), I64(i64), F32(f32), F64(f64),
    Text(Text<'a>), Bytes(Bytes<'a>), Date(i64), Decimal { coefficient: [u8; 32], width: usize, scale: i32 },
    DecimalNumber(&'a JsonDecimal, Scalar),
}
#[derive(Debug)]
enum Text<'a> { Borrowed(&'a str), Double(f64), Decimal(&'a JsonDecimal) }
#[derive(Clone, Copy, Debug)]
enum Bytes<'a> { Borrowed(&'a [u8]), Latin1(&'a str) }
enum Octets<'a> { Borrowed(std::iter::Copied<std::slice::Iter<'a, u8>>), Latin1(std::str::Chars<'a>) }
impl Iterator for Octets<'_> {
    type Item = u8;
    fn next(&mut self) -> Option<u8> { match self { Self::Borrowed(value) => value.next(), Self::Latin1(value) => value.next().map(latin1) } }
}

impl<'a> Bytes<'a> {
    fn len(self) -> usize { match self { Self::Borrowed(value) => value.len(), Self::Latin1(value) => value.chars().count() } }
    fn octets(self) -> Octets<'a> { match self { Self::Borrowed(value) => Octets::Borrowed(value.iter().copied()), Self::Latin1(value) => Octets::Latin1(value.chars()) } }
    fn materialize(self) -> Vec<u8> {
        match self {
            Self::Borrowed(value) => value.to_vec(),
            Self::Latin1(value) => { let mut bytes = Vec::with_capacity(self.len()); bytes.extend(value.chars().map(latin1)); bytes }
        }
    }
}
fn latin1(value: char) -> u8 { u8::try_from(u32::from(value)).unwrap_or(b'?') }
fn invalid<T>(message: &'static str) -> DefaultAttempt<T> { DefaultAttempt::Invalid(error("avro-schema", message)) }
fn overflow() -> Anomaly { error("allocation-overflow", "Default scalar allocation overflow") }
fn unsupported(message: &'static str) -> Anomaly { error("unsupported-avro-default", message) }
fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(overflow) }
fn mul(left: usize, right: usize) -> Result<usize> { left.checked_mul(right).ok_or_else(overflow) }

pub(in crate::connect::avro) fn prepare<'a>(input: ScalarInput<'a>, destination: SchemaView<'_>, path: ConversionPath, logical_name: Option<&str>) -> Result<DefaultAttempt<PreparedScalar<'a>>> {
    let object = match input {
        ScalarInput::FieldObject(value) => {
            let value = value.kind()?;
            if matches!(value, ObjectKind::Null) { return Ok(DefaultAttempt::Value(PreparedScalar { value: PreparedValue::Null })); }
            Some(value)
        }
        ScalarInput::Native(Value::Null) => return Ok(DefaultAttempt::Value(PreparedScalar { value: PreparedValue::Null })),
        ScalarInput::JsonNode(value) if value.raw().is_none() => return Ok(DefaultAttempt::Value(PreparedScalar { value: PreparedValue::Missing })),
        ScalarInput::JsonNode(value) if matches!(value.schema(), Schema::Null) => return Ok(DefaultAttempt::Value(PreparedScalar { value: PreparedValue::Null })),
        _ => None,
    };
    let ViewKind::Scalar(kind) = destination.kind() else { return Err(unsupported("Default scalar preparation requires a scalar destination")); };
    let value = match input {
        ScalarInput::JsonNode(node) => {
            if matches!(path, ConversionPath::MapChild) { return Err(unsupported("A JsonNode default requires its wrapper conversion boundary")); }
            json_node(node, kind)?
        }
        ScalarInput::FieldObject(_) => match field_object(object.expect("prepared field-object class"), kind) {
            DefaultAttempt::Value(value) => value,
            DefaultAttempt::Invalid(cause) => return Ok(DefaultAttempt::Invalid(cause)),
        },
        ScalarInput::Native(value) => match native(value, kind) {
            DefaultAttempt::Value(value) => value,
            DefaultAttempt::Invalid(cause) => return Ok(DefaultAttempt::Invalid(cause)),
        },
    };
    let value = match logical(value, logical_name, destination)? {
        DefaultAttempt::Value(value) => value,
        DefaultAttempt::Invalid(cause) => return Ok(DefaultAttempt::Invalid(cause)),
    };
    Ok(DefaultAttempt::Value(PreparedScalar { value }))
}

fn field_object<'a>(value: ObjectKind<'a>, kind: Scalar) -> DefaultAttempt<PreparedValue<'a>> {
    use PreparedValue as P;
    let value = match (kind, value) {
        (_, ObjectKind::Null) => P::Null,
        (Scalar::Bool, ObjectKind::Boolean(value)) => P::Bool(value),
        (Scalar::I8, ObjectKind::Int(value)) => P::I8(value as i8),
        (Scalar::I16, ObjectKind::Int(value)) => P::I16(value as i16),
        (Scalar::I32, ObjectKind::Int(value)) => P::I32(value),
        (Scalar::I64, ObjectKind::Long(value)) => P::I64(value),
        (Scalar::F32, ObjectKind::Float(value)) => P::F32(value),
        (Scalar::F64, ObjectKind::Double(value)) => P::F64(value),
        (Scalar::String, ObjectKind::Text(value)) => P::Text(Text::Borrowed(value)),
        (Scalar::Bytes, ObjectKind::BinaryText(value)) => P::Bytes(Bytes::Latin1(value)),
        _ => return invalid("Invalid Java object class for default scalar"),
    };
    DefaultAttempt::Value(value)
}
fn native(value: &Value, kind: Scalar) -> DefaultAttempt<PreparedValue<'_>> {
    use PreparedValue as P;
    let value = match (kind, value) {
        (_, Value::Null) => P::Null,
        (Scalar::Bool, Value::Bool(value)) => P::Bool(*value),
        (Scalar::I8, Value::I32(value)) => P::I8(*value as i8),
        (Scalar::I16, Value::I32(value)) => P::I16(*value as i16),
        (Scalar::I32, Value::I32(value)) => P::I32(*value),
        (Scalar::I64, Value::I64(value)) => P::I64(*value),
        (Scalar::F32, Value::F32(value)) => P::F32(*value),
        (Scalar::F64, Value::F64(value)) => P::F64(*value),
        (Scalar::String, Value::Utf8(value)) => P::Text(Text::Borrowed(value)),
        (Scalar::Bytes, Value::Binary(value)) => P::Bytes(Bytes::Borrowed(value)),
        _ => return invalid("Invalid Java object class for default scalar"),
    };
    DefaultAttempt::Value(value)
}

fn json_node<'a>(node: JsonNode<'a>, kind: Scalar) -> Result<PreparedValue<'a>> {
    use PreparedValue as P;
    let Some(raw) = node.raw() else { return Ok(P::Missing); };
    Ok(match node.schema() {
        Schema::Int | Schema::Date | Schema::TimeMillis => match kind {
            Scalar::I8 => P::I8(raw.int_value().unwrap_or(0) as i8),
            Scalar::I16 => P::I16(raw.int_value().unwrap_or(0) as i16),
            Scalar::I32 => P::I32(raw.int_value().unwrap_or(0)),
            _ => P::Null,
        },
        Schema::Long | Schema::TimeMicros | Schema::TimestampMillis | Schema::TimestampMicros | Schema::TimestampNanos | Schema::LocalTimestampMillis | Schema::LocalTimestampMicros | Schema::LocalTimestampNanos => P::I64(raw.long_value().unwrap_or(0)),
        Schema::Float => match raw {
            DefaultJson::Decimal(value) => P::DecimalNumber(value, Scalar::F32),
            _ => P::F32(raw.double_value().unwrap_or(0.0) as f32),
        },
        Schema::Double => match raw {
            DefaultJson::Decimal(value) => P::DecimalNumber(value, Scalar::F64),
            _ => P::F64(raw.double_value().unwrap_or(0.0)),
        },
        Schema::Boolean => P::Bool(json_boolean(raw)),
        Schema::String | Schema::Uuid | Schema::Enum(_) => P::Text(json_text(raw)),
        Schema::Bytes | Schema::Fixed(_) | Schema::Duration | Schema::BigDecimal => match raw { DefaultJson::String(value) => P::Bytes(Bytes::Latin1(value)), _ => P::Null },
        Schema::Decimal(value) => match value.inner.as_ref() {
            Schema::Bytes | Schema::Fixed(_) => match raw { DefaultJson::String(value) => P::Bytes(Bytes::Latin1(value)), _ => P::Null },
            _ => return Err(unsupported("Decimal default has an unsupported physical schema")),
        },
        Schema::Null => P::Null,
        Schema::Ref { .. } | Schema::Union(_) => return Err(unsupported("Default scalar requires a resolved nonunion physical schema")),
        Schema::Array(_) | Schema::Map(_) | Schema::Record(_) => return Err(unsupported("A container default requires the container traversal")),
    })
}
fn json_boolean(value: &DefaultJson) -> bool {
    match value {
        DefaultJson::Boolean(value) => *value,
        DefaultJson::Number(value) if !value.contains(['.', 'e', 'E']) => value.bytes().any(|byte| matches!(byte, b'1'..=b'9')),
        DefaultJson::String(value) => value.trim_matches(|character: char| u32::from(character) <= 0x20) == "true",
        _ => false,
    }
}
fn json_text(value: &DefaultJson) -> Text<'_> {
    Text::Borrowed(match value {
        DefaultJson::Null => "null",
        DefaultJson::Boolean(true) => "true",
        DefaultJson::Boolean(false) => "false",
        DefaultJson::String(value) => value,
        DefaultJson::Number(value) if !value.contains(['.', 'e', 'E']) => if value == "-0" { "0" } else { value },
        DefaultJson::Number(_) => return Text::Double(value.double_value().unwrap_or(0.0)),
        DefaultJson::Decimal(value) => return Text::Decimal(value),
        DefaultJson::Array(_) | DefaultJson::Object(_) => "",
    })
}

fn logical<'a>(value: PreparedValue<'a>, name: Option<&str>, schema: SchemaView<'_>) -> Result<DefaultAttempt<PreparedValue<'a>>> {
    use PreparedValue as P;
    if matches!(value, P::Null | P::Missing) { return Ok(DefaultAttempt::Value(value)); }
    let value = match (name, value) {
        (Some("org.apache.kafka.connect.data.Date"), P::I32(value)) => P::Date(i64::from(value) * 86_400_000),
        (Some("org.apache.kafka.connect.data.Time"), P::I32(value)) => {
            if !(0..=86_400_000).contains(&value) { return Ok(invalid("Time default is outside one day")); }
            P::Date(i64::from(value))
        }
        (Some("org.apache.kafka.connect.data.Timestamp"), P::I64(value)) => P::Date(value),
        (Some("org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time" | "org.apache.kafka.connect.data.Timestamp"), _) => return Ok(invalid("Logical date default has an incompatible primitive schema")),
        (Some("org.apache.kafka.connect.data.Decimal"), P::Bytes(bytes)) => return decimal(bytes, schema),
        (Some("org.apache.kafka.connect.data.Decimal"), _) => return Ok(invalid("Logical Decimal default has an incompatible primitive representation")),
        (_, value) => value,
    };
    Ok(DefaultAttempt::Value(value))
}

fn decimal<'a>(bytes: Bytes<'a>, schema: SchemaView<'_>) -> Result<DefaultAttempt<PreparedValue<'a>>> {
    let mut octets = bytes.octets().peekable();
    let Some(mut first) = octets.next() else { return Err(error("avro-default-unchecked", "Decimal coefficient bytes are empty")); };
    let Some(scale) = schema.parameter("scale") else { return Ok(invalid("Decimal schema has no scale")); };
    let Some(scale) = scale_value(scale) else { return Ok(invalid("Decimal scale is not a signed 32-bit integer")); };
    while let Some(&next) = octets.peek() {
        if first == 0 && next & 128 == 0 || first == 255 && next & 128 != 0 { first = octets.next().expect("peeked coefficient octet"); } else { break; }
    }
    let negative = first & 128 != 0;
    let mut coefficient = [if negative { 255 } else { 0 }; 32];
    coefficient[0] = first;
    let mut significant = 1usize;
    for value in octets {
        if significant == 32 { return Err(unsupported("Decimal precision exceeds the reference writer limit")); }
        coefficient[significant] = value;
        significant += 1;
    }
    coefficient.copy_within(..significant, 32 - significant);
    coefficient[..32 - significant].fill(if negative { 255 } else { 0 });
    let mut magnitude = [0u8; 32];
    if negative {
        let mut carry = true;
        for offset in (0..32).rev() { let (value, overflow) = (!coefficient[offset]).overflowing_add(u8::from(carry)); magnitude[offset] = value; carry = overflow; }
    } else { magnitude = coefficient; }
    const LIMIT32: [u8; 32] = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,4,238,45,109,65,91,133,172,239,129,0,0,0,0];
    const LIMIT64: [u8; 32] = [0,0,0,0,0,24,79,3,233,63,249,244,218,167,151,237,110,56,237,100,191,106,31,1,0,0,0,0,0,0,0,0];
    let width = if magnitude < LIMIT32 { 16 } else if magnitude < LIMIT64 { 32 } else { return Err(unsupported("Decimal precision exceeds the reference writer limit")); };
    Ok(DefaultAttempt::Value(PreparedValue::Decimal { coefficient, width, scale }))
}
fn scale_value(text: &str) -> Option<i32> {
    let (negative, digits) = if let Some(digits) = text.strip_prefix('-') { (true, digits) } else { (false, text.strip_prefix('+').unwrap_or(text)) };
    if digits.is_empty() { return None; }
    let magnitude = digits.chars().try_fold(0i64, |value, digit| {
        let number = rtdb_types::decimal_digit(digit).filter(|_| digit.len_utf16() == 1)?;
        value.checked_mul(10)?.checked_add(i64::from(number)).filter(|value| *value <= i64::from(i32::MAX) + i64::from(negative))
    })?;
    Some(if negative { -magnitude } else { magnitude } as i32)
}

pub(in crate::connect::avro) fn validate_string_class(destination: SchemaView<'_>, logical_name: Option<&str>) -> DefaultAttempt<()> {
    PreparedScalar { value: PreparedValue::Text(Text::Borrowed("")) }.validate_destination(destination, logical_name)
}

impl PreparedScalar<'_> {
    pub(in crate::connect::avro) fn is_missing(&self) -> bool { matches!(self.value, PreparedValue::Missing) }
    pub(in crate::connect::avro) fn is_null(&self) -> bool { matches!(self.value, PreparedValue::Null) }
    pub(in crate::connect::avro) fn validate_destination(&self, destination: SchemaView<'_>, logical_name: Option<&str>) -> DefaultAttempt<()> {
        use PreparedValue as P;
        if self.is_missing() || self.is_null() { return DefaultAttempt::Value(()); }
        let valid = match logical_name {
            Some("org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time" | "org.apache.kafka.connect.data.Timestamp") => matches!(self.value, P::Date(_)),
            Some("org.apache.kafka.connect.data.Decimal") => matches!(self.value, P::Decimal { .. }),
            _ => matches!((destination.kind(), &self.value),
                (ViewKind::Scalar(Scalar::Bool), P::Bool(_)) |
                (ViewKind::Scalar(Scalar::I8), P::I8(_)) | (ViewKind::Scalar(Scalar::I16), P::I16(_)) |
                (ViewKind::Scalar(Scalar::I32), P::I32(_)) | (ViewKind::Scalar(Scalar::I64), P::I64(_)) |
                (ViewKind::Scalar(Scalar::F32), P::F32(_) | P::DecimalNumber(_, Scalar::F32)) |
                (ViewKind::Scalar(Scalar::F64), P::F64(_) | P::DecimalNumber(_, Scalar::F64)) |
                (ViewKind::Scalar(Scalar::String), P::Text(_)) | (ViewKind::Scalar(Scalar::Bytes), P::Bytes(_))),
        };
        if valid { DefaultAttempt::Value(()) } else { invalid("Default value does not match its destination class") }
    }
    pub(in crate::connect::avro) fn bound(&self) -> Result<usize> {
        let payload = match &self.value {
            PreparedValue::Text(Text::Borrowed(value)) => value.len(),
            PreparedValue::Text(Text::Double(_)) => 4096,
            PreparedValue::Text(Text::Decimal(value)) | PreparedValue::DecimalNumber(value, _) => add(mul(value.encoded_bound(), 4)?, 4096)?,
            PreparedValue::Bytes(value) => value.len(),
            PreparedValue::Date(_) => 3,
            PreparedValue::Decimal { width, .. } => *width,
            _ => 0,
        };
        add(size_of::<Value>(), add(payload, 128)?)
    }
    /// The caller admits bound() and retains that reservation through the returned value's lifetime.
    pub(in crate::connect::avro) fn materialize(self) -> Result<Value> {
        Ok(match self.value {
            PreparedValue::Missing | PreparedValue::Null => Value::Null,
            PreparedValue::Bool(value) => Value::Bool(value),
            PreparedValue::I8(value) => Value::I8(value), PreparedValue::I16(value) => Value::I16(value),
            PreparedValue::I32(value) => Value::I32(value), PreparedValue::I64(value) => Value::I64(value),
            PreparedValue::F32(value) => Value::F32(value), PreparedValue::F64(value) => Value::F64(value),
            PreparedValue::Text(value) => Value::Utf8(match value { Text::Borrowed(value) => value.to_owned(), Text::Double(value) => rtdb_types::java_f64_text(value), Text::Decimal(value) => value.text() }),
            PreparedValue::Bytes(value) => Value::Binary(value.materialize()),
            PreparedValue::Date(value) => super::super::data::date_value(value),
            PreparedValue::Decimal { coefficient, width, scale } => Value::Decimal(DecimalValue::new(coefficient[32-width..].to_vec(), (width*2) as i32, scale, (width*8) as u16)?),
            PreparedValue::DecimalNumber(value, Scalar::F32) => Value::F32(value.double_value() as f32),
            PreparedValue::DecimalNumber(value, Scalar::F64) => Value::F64(value.double_value()),
            PreparedValue::DecimalNumber(_, _) => unreachable!("prepared floating Decimal accessor"),
        })
    }
}

#[cfg(test)] mod tests;
