use std::collections::BTreeMap;
use apache_avro::Schema;
use rtdb_types::{Anomaly, Result};
use super::{default_json::DefaultJson, error, names::LookupName};
use super::super::json_value::JsonObject;

#[derive(Debug)]
pub(super) enum DefaultAttempt<T> { Value(T), Invalid(Anomaly) }
#[derive(Clone, Copy)]
pub(super) struct FieldObject<'a> { value: &'a DefaultJson, schema: Option<&'a Schema>, names: &'a BTreeMap<String, Schema> }
#[derive(Clone, Copy)]
pub(super) struct JsonNode<'a> { value: Option<&'a DefaultJson>, schema: &'a Schema }
pub(super) enum DefaultInput<'a> { AvroFieldObject(FieldObject<'a>), ConnectJsonNode(JsonNode<'a>) }

pub(super) enum ObjectKind<'a> {
    Null, Boolean(bool), Int(i32), Long(i64), Float(f32), Double(f64), Text(&'a str), BinaryText(&'a str),
    Array(&'a [DefaultJson]), Object(&'a JsonObject),
}
fn invalid<T>(message: &'static str) -> DefaultAttempt<T> { DefaultAttempt::Invalid(error("avro-schema", message)) }
enum Physical<'a> { Untyped, Schema(&'a Schema), EmptyUnion }
fn physical<'a>(mut schema: Option<&'a Schema>, names: &'a BTreeMap<String, Schema>) -> Result<Physical<'a>> {
    let mut depth=0usize;
    loop {
        if depth>128 { return Err(error("batch-too-large", "Default schema nesting limit exceeded")); }
        schema=match schema {
            Some(schema @ Schema::Ref {..}) => Some(super::resolve(schema,names)?),
            Some(Schema::Decimal(value)) => Some(value.inner.as_ref()),
            Some(Schema::Union(value)) => match value.variants().first() { Some(schema) => Some(schema),None => return Ok(Physical::EmptyUnion) },
            _ => return Ok(match schema {Some(schema) => Physical::Schema(schema),None => Physical::Untyped}),
        };
        depth+=1;
    }
}
fn prepared_schema<'a>(schema: Option<&'a Schema>, names: &'a BTreeMap<String, Schema>) -> Result<Option<&'a Schema>> {
    match physical(schema,names)? { Physical::Untyped => Ok(None),Physical::Schema(schema) => Ok(Some(schema)),Physical::EmptyUnion => Err(error("avro-schema", "Prepared default contains an empty union")) }
}
impl<'a> FieldObject<'a> {
    pub(super) fn prepare(value: &'a DefaultJson, schema: &'a Schema, names: &'a BTreeMap<String, Schema>) -> Result<DefaultAttempt<Self>> {
        let input=Self { value,schema:Some(schema),names };
        match input.validate_source(0)? { DefaultAttempt::Value(()) => Ok(DefaultAttempt::Value(input)),DefaultAttempt::Invalid(cause) => Ok(DefaultAttempt::Invalid(cause)) }
    }
    pub(super) fn kind(self) -> Result<ObjectKind<'a>> {
        let schema=prepared_schema(self.schema,self.names)?;
        Ok(match self.value {
            DefaultJson::Null => ObjectKind::Null,
            DefaultJson::Boolean(value) => ObjectKind::Boolean(*value),
            DefaultJson::Number(number) if !number.contains(['.','e','E']) => match number.parse::<i64>() {
                Ok(value) => match schema {
                    None => if i32::try_from(value).is_ok() { ObjectKind::Int(value as i32) } else { ObjectKind::Long(value) },
                    Some(Schema::Int | Schema::Date | Schema::TimeMillis) => if i32::try_from(value).is_ok() { ObjectKind::Int(value as i32) } else { ObjectKind::Long(value) },
                    Some(Schema::Long | Schema::TimestampMillis | Schema::TimeMicros | Schema::TimestampMicros | Schema::TimestampNanos | Schema::LocalTimestampMillis | Schema::LocalTimestampMicros | Schema::LocalTimestampNanos) => ObjectKind::Long(value),
                    Some(Schema::Float) => ObjectKind::Float(self.value.double_value().unwrap_or(0.0) as f32),
                    Some(Schema::Double) => ObjectKind::Double(self.value.double_value().unwrap_or(0.0)),
                    _ => ObjectKind::Null,
                },
                Err(_) => ObjectKind::Null,
            },
            DefaultJson::Number(_) => match schema {
                None | Some(Schema::Double) => ObjectKind::Double(self.value.double_value().unwrap_or(0.0)),
                Some(Schema::Float) => ObjectKind::Float(self.value.double_value().unwrap_or(0.0) as f32),
                _ => ObjectKind::Null,
            },
            DefaultJson::Decimal(_) => ObjectKind::Null,
            DefaultJson::String(value) => match schema {
                None | Some(Schema::String | Schema::Uuid | Schema::Enum(_)) => ObjectKind::Text(value),
                Some(Schema::Bytes | Schema::BigDecimal | Schema::Fixed(_) | Schema::Duration) => ObjectKind::BinaryText(value),
                _ => ObjectKind::Null,
            },
            DefaultJson::Array(values) => ObjectKind::Array(values),
            DefaultJson::Object(values) => ObjectKind::Object(values),
        })
    }
    fn validate_source(self, depth: usize) -> Result<DefaultAttempt<()>> {
        if depth>128 { return Err(error("batch-too-large", "Default object nesting limit exceeded")); }
        let schema=match physical(self.schema,self.names)? { Physical::Untyped => None,Physical::Schema(schema) => Some(schema),Physical::EmptyUnion => return Ok(invalid("Default union has no first member")) };
        match self.value {
            DefaultJson::Array(values) => {
                if values.is_empty() { return Ok(DefaultAttempt::Value(())); }
                let element=match schema { None => None,Some(Schema::Array(array)) => Some(array.items.as_ref()),_ => return Ok(invalid("Array default source schema is not an array")) };
                for value in values { if let DefaultAttempt::Invalid(cause)=(Self { value,schema:element,names:self.names }).validate_source(depth+1)? { return Ok(DefaultAttempt::Invalid(cause)); } }
            }
            DefaultJson::Object(values) => {
                for (key,value) in values {
                    let child=match schema {
                        Some(Schema::Map(map)) => Some(map.types.as_ref()),
                        Some(Schema::Record(record)) => match record.fields.iter().find(|field| field.name==*key) { Some(field) => Some(&field.schema),None => return Ok(invalid("Record default source field is absent")) },
                        _ => None,
                    };
                    if let DefaultAttempt::Invalid(cause)=(Self { value,schema:child,names:self.names }).validate_source(depth+1)? { return Ok(DefaultAttempt::Invalid(cause)); }
                }
            }
            _ => (),
        }
        Ok(DefaultAttempt::Value(()))
    }
    pub(super) fn field(self, destination_name: &str, scrub: bool) -> Result<Option<Self>> {
        let DefaultJson::Object(values)=self.value else { return Ok(None); };
        let Some((name,value))=values.iter().find(|(key,_)| LookupName::matches(destination_name,key,scrub)) else { return Ok(None); };
        let schema=match prepared_schema(self.schema,self.names)? {
            Some(Schema::Record(record)) => record.fields.iter().find(|field| field.name==*name).map(|field| &field.schema),
            Some(Schema::Map(map)) => Some(map.types.as_ref()),
            _ => None,
        };
        Ok(Some(Self {value,schema,names:self.names}))
    }
    pub(super) fn raw(self) -> &'a DefaultJson { self.value }
}
impl<'a> JsonNode<'a> {
    pub(super) fn new(value: Option<&'a DefaultJson>, schema: &'a Schema) -> Self { Self {value,schema} }
    pub(super) fn field(self, raw_name: &str, field_schema: &'a Schema) -> Self { Self::new(self.value.and_then(|value| value.get(raw_name)),field_schema) }
    pub(super) fn raw(self) -> Option<&'a DefaultJson> { self.value }
    pub(super) fn schema(self) -> &'a Schema { self.schema }
}

#[cfg(test)] mod tests;
