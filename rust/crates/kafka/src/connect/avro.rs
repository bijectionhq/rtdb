use std::{collections::BTreeMap, io::Cursor, sync::Arc};
use apache_avro::{Schema, types::Value as Avro};
use rtdb_types::{DecimalValue, Result, TimeUnit, Value};
use super::{ConnectData, ConnectSchema, ConversionContext, SchemaKind, error};
mod metadata;
mod graph;
mod union;

pub(super) struct AvroPlan { root: Schema, references: Vec<Schema>, names: BTreeMap<String, Schema>, connect: Arc<ConnectSchema>, unions: graph::UnionOptions }
impl AvroPlan {
    #[cfg(test)]
    pub fn parse(text: &str, references: &[String]) -> Result<Self> { Self::parse_version(text, references, &BTreeMap::new(), None) }
    #[cfg(test)]
    pub fn parse_version(text: &str, references: &[String], options: &BTreeMap<String, String>, version: Option<i32>) -> Result<Self> {
        Self::parse_version_index(text, references, options, version, &mut 0)
    }
    pub fn parse_version_index(text: &str, references: &[String], options: &BTreeMap<String, String>, version: Option<i32>, next_union: &mut i32) -> Result<Self> {
        let metadata = metadata::Metadata::new(text, references, options, version)?;
        let unions = graph::UnionOptions::parse(options)?;
        let text = physical_logical_types(text)?;
        let references = references.iter().map(|text| physical_logical_types(text)).collect::<Result<Vec<_>>>()?;
        let (root, references) = Schema::parse_str_with_list(&text, &references).map_err(|cause| error("avro-schema", cause))?;
        let mut names = BTreeMap::new(); collect_names(&root, &mut names);
        for reference in &references { collect_names(reference, &mut names); }
        let connect = graph::build(&root, &names, &metadata, unions, next_union)?;
        Ok(Self { root, references, names, connect, unions })
    }
    pub fn decode(&self, bytes: &[u8], context: &mut ConversionContext) -> Result<ConnectData> {
        if matches!(&self.root, Schema::Bytes) || matches!(&self.root, Schema::Decimal(value) if matches!(value.inner.as_ref(), Schema::Bytes)) {
            context.reserve(bytes.len().checked_mul(4).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| error("allocation-overflow", "Avro byte value allocation overflow"))?)?;
            let value = match &self.root { Schema::Decimal(value) => decimal_value(bytes.to_vec(), value.scale)?, _ => Value::Binary(bytes.to_vec()) };
            return Ok(ConnectData { schema: Some(Arc::clone(&self.connect)), value });
        }
        let mut preflight = Preflight { bytes, offset: 0, allocated: 4096, limit: context.allocation_limit(), names: &self.names, view_bytes: self.connect.view_allocation_bound() };
        preflight.value(&self.root, 0)?;
        if preflight.offset != bytes.len() { return Err(error("avro-decode", "Trailing bytes after Avro datum")); }
        context.reserve(preflight.allocated)?;
        let mut reader = Cursor::new(bytes);
        let mut schemata = self.references.iter().collect::<Vec<_>>(); schemata.push(&self.root);
        let value = apache_avro::from_avro_datum_schemata(&self.root, schemata, &mut reader, None).map_err(|cause| error("avro-decode", cause))?;
        let value = metadata::coerce(convert(&self.root, value, &self.names, self.unions, &self.connect, 0)?, &self.connect)?;
        Ok(ConnectData { schema: Some(Arc::clone(&self.connect)), value })
    }
}
fn collect_names(schema: &Schema, names: &mut BTreeMap<String, Schema>) {
    match schema {
        Schema::Record(record) => { names.insert(record.name.fullname(None), schema.clone()); for field in &record.fields { collect_names(&field.schema, names); } }
        Schema::Enum(value) => { names.insert(value.name.fullname(None), schema.clone()); }
        Schema::Fixed(value) => { names.insert(value.name.fullname(None), schema.clone()); }
        Schema::Array(value) => collect_names(&value.items, names), Schema::Map(value) => collect_names(&value.types, names),
        Schema::Union(value) => for schema in value.variants() { collect_names(schema, names); },
        Schema::Decimal(value) => { if let Schema::Fixed(fixed) = value.inner.as_ref() { names.insert(fixed.name.fullname(None), schema.clone()); } else { collect_names(&value.inner, names); } }, _ => (),
    }
}
fn resolve<'a>(schema: &'a Schema, names: &'a BTreeMap<String, Schema>) -> Result<&'a Schema> {
    match schema { Schema::Ref { name } => names.get(&name.fullname(None)).ok_or_else(|| error("avro-schema", format!("Unknown schema reference '{}'", name.fullname(None)))), _ => Ok(schema) }
}
fn member_name(schema: &Schema) -> String {
    match schema {
        Schema::Record(value) => return value.name.name.clone(), Schema::Enum(value) => return value.name.name.clone(), Schema::Fixed(value) => return value.name.name.clone(),
        Schema::Null => "null", Schema::Boolean => "boolean", Schema::Int | Schema::Date | Schema::TimeMillis => "int",
        Schema::Long | Schema::TimeMicros | Schema::TimestampMillis | Schema::TimestampMicros | Schema::TimestampNanos | Schema::LocalTimestampMillis | Schema::LocalTimestampMicros | Schema::LocalTimestampNanos => "long",
        Schema::Float => "float", Schema::Double => "double", Schema::String | Schema::Uuid => "string", Schema::Bytes | Schema::BigDecimal => "bytes",
        Schema::Decimal(value) => return member_name(&value.inner),
        Schema::Array(_) => "array", Schema::Map(_) => "map", Schema::Union(_) => "union", Schema::Duration => "fixed", Schema::Ref { name } => return name.name.clone(),
    }.to_owned()
}
fn physical_logical_types(text: &str) -> Result<String> {
    fn normalize(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Array(values) => for value in values { normalize(value); },
            serde_json::Value::Object(fields) => {
                if let Some(name) = fields.get("logicalType").and_then(serde_json::Value::as_str).map(str::to_ascii_lowercase) {
                    if matches!(name.as_str(), "decimal" | "date" | "time-millis" | "timestamp-millis") { fields.insert("logicalType".into(), name.into()); } else { fields.remove("logicalType"); }
                }
                for value in fields.values_mut() { normalize(value); }
            }
            _ => (),
        }
    }
    let mut value = serde_json::from_str(text).map_err(|cause| error("avro-schema", cause))?; normalize(&mut value);
    serde_json::to_string(&value).map_err(|cause| error("avro-schema", cause))
}
struct Preflight<'a> { bytes: &'a [u8], offset: usize, allocated: usize, limit: usize, names: &'a BTreeMap<String, Schema>, view_bytes: usize }
impl Preflight<'_> {
    fn claim(&mut self, bytes: usize) -> Result<()> {
        self.allocated = self.allocated.checked_add(bytes).filter(|bytes| *bytes <= self.limit).ok_or_else(|| error("batch-too-large", "Avro decoded allocation exceeds its limit"))?; Ok(())
    }
    fn take(&mut self, count: usize) -> Result<&[u8]> {
        let end = self.offset.checked_add(count).ok_or_else(|| error("avro-decode", "Avro length overflow"))?;
        let bytes = self.bytes.get(self.offset..end).ok_or_else(|| error("avro-decode", "Truncated Avro datum"))?; self.offset = end; Ok(bytes)
    }
    fn long(&mut self) -> Result<i64> {
        let mut value = 0u64;
        for shift in (0..70).step_by(7) {
            let byte = self.take(1)?[0];
            if shift == 63 && byte > 1 { return Err(error("avro-decode", "Avro long exceeds 64 bits")); }
            value |= u64::from(byte & 127) << shift;
            if byte & 128 == 0 { return Ok(((value >> 1) as i64) ^ -((value & 1) as i64)); }
        }
        Err(error("avro-decode", "Invalid Avro varint"))
    }
    fn bytes(&mut self) -> Result<()> {
        let count = usize::try_from(self.long()?).map_err(|_| error("avro-decode", "Negative Avro byte count"))?;
        self.claim(count.checked_mul(4).ok_or_else(|| error("avro-decode", "Avro byte count overflow"))?)?; self.take(count)?; Ok(())
    }
    fn value(&mut self, schema: &Schema, depth: usize) -> Result<()> {
        if depth > 128 { return Err(error("avro-decode", "Avro datum nesting limit exceeded")); }
        self.claim(4096usize.saturating_add(self.view_bytes))?;
        match resolve(schema, self.names)? {
            Schema::Null => (), Schema::Boolean => { self.take(1)?; }
            Schema::Int | Schema::Long | Schema::Date | Schema::TimeMillis | Schema::TimeMicros | Schema::TimestampMillis | Schema::TimestampMicros | Schema::TimestampNanos | Schema::LocalTimestampMillis | Schema::LocalTimestampMicros | Schema::LocalTimestampNanos | Schema::Enum(_) => { self.long()?; }
            Schema::Float => { self.take(4)?; } Schema::Double => { self.take(8)?; }
            Schema::Bytes | Schema::String | Schema::Uuid | Schema::BigDecimal => self.bytes()?,
            Schema::Fixed(value) => { self.claim(value.size.saturating_mul(4))?; self.take(value.size)?; }
            Schema::Duration => { self.take(12)?; }
            Schema::Decimal(value) => self.value(&value.inner, depth + 1)?,
            Schema::Record(record) => for field in &record.fields { self.claim(field.name.len().saturating_mul(4))?; self.value(&field.schema, depth + 1)?; },
            Schema::Union(union) => {
                self.claim(union.variants().len().checked_mul(self.view_bytes.saturating_add(4096)).ok_or_else(|| error("allocation-overflow", "Avro union view allocation exceeds range"))?)?;
                let index = usize::try_from(self.long()?).map_err(|_| error("avro-decode", "Negative Avro union index"))?;
                let selected = union.variants().get(index).ok_or_else(|| error("avro-decode", "Avro union index is out of range"))?;
                self.value(selected, depth + 1)?;
            }
            Schema::Array(array) => self.collection(&array.items, false, depth + 1)?,
            Schema::Map(map) => self.collection(&map.types, true, depth + 1)?,
            Schema::Ref { .. } => return Err(error("avro-schema", "Unresolved schema reference")),
        }
        Ok(())
    }
    fn collection(&mut self, schema: &Schema, map: bool, depth: usize) -> Result<()> {
        loop {
            let count = self.long()?; if count == 0 { return Ok(()); }
            let (count, block_end) = if count < 0 {
                let count = count.checked_neg().ok_or_else(|| error("avro-decode", "Avro collection count overflow"))?;
                let size = usize::try_from(self.long()?).map_err(|_| error("avro-decode", "Negative Avro block byte count"))?;
                (count, Some(self.offset.checked_add(size).ok_or_else(|| error("avro-decode", "Avro block length overflow"))?))
            } else { (count, None) };
            let count = usize::try_from(count).map_err(|_| error("avro-decode", "Avro collection count overflow"))?;
            self.claim(count.checked_mul(4096).ok_or_else(|| error("avro-decode", "Avro collection allocation overflow"))?)?;
            for _ in 0..count { if map { self.bytes()?; } self.value(schema, depth)?; }
            if block_end.is_some_and(|end| end != self.offset) { return Err(error("avro-decode", "Avro block byte count disagrees with its contents")); }
        }
    }
}
fn convert(schema: &Schema, value: Avro, names: &BTreeMap<String, Schema>, unions: graph::UnionOptions, connect: &ConnectSchema, depth: usize) -> Result<Value> {
    if depth > 128 { return Err(error("avro-decode", "Avro datum nesting limit exceeded")); }
    let schema = resolve(schema, names)?;
    let connect = connect.resolved();
    Ok(match value {
        Avro::Null => Value::Null, Avro::Boolean(value) => Value::Bool(value), Avro::Int(value) => Value::I32(value), Avro::Long(value) => Value::I64(value),
        Avro::Float(value) => Value::F32(value), Avro::Double(value) => Value::F64(value), Avro::String(value) | Avro::Enum(_, value) => Value::Utf8(value),
        Avro::Bytes(value) | Avro::Fixed(_, value) => match schema { Schema::Decimal(decimal) => decimal_value(value, decimal.scale)?, _ => Value::Binary(value) },
        Avro::Date(value) => Value::DateDay(value), Avro::TimeMillis(value) => {
            if !(0..86_400_000).contains(&value) { return Err(error("avro-decode", "Time is outside one day")); }
            Value::Time { value: i64::from(value), unit: TimeUnit::Millisecond, bit_width: 32 }
        },
        Avro::TimestampMillis(value) => Value::Timestamp { value, unit: TimeUnit::Millisecond, timezone: Some("UTC".into()) },
        Avro::TimeMicros(value) | Avro::TimestampMicros(value) | Avro::TimestampNanos(value) | Avro::LocalTimestampMillis(value) | Avro::LocalTimestampMicros(value) | Avro::LocalTimestampNanos(value) => Value::I64(value),
        Avro::Uuid(value) => Value::Utf8(value.to_string()),
        Avro::Decimal(value) => {
            let bytes = Vec::<u8>::try_from(value).map_err(|cause| error("avro-decode", cause))?;
            match schema { Schema::Decimal(decimal) => decimal_value(bytes, decimal.scale)?, _ => Value::Binary(bytes) }
        }
        Avro::Array(values) => {
            let Schema::Array(array) = schema else { return Err(error("avro-decode", "Array value lacks its schema")); };
            let element = match &connect.kind { SchemaKind::Array(element) => element.as_ref(), SchemaKind::Map(_, _) => connect.as_ref(), _ => return Err(error("avro-decode", "Array Connect schema is absent")) };
            Value::List(values.into_iter().map(|value| convert(&array.items, value, names, unions, element, depth + 1)).collect::<Result<_>>()?)
        }
        Avro::Map(values) => {
            let Schema::Map(map) = schema else { return Err(error("avro-decode", "Map value lacks its schema")); };
            let SchemaKind::Map(_, element) = &connect.kind else { return Err(error("avro-decode", "Map Connect schema is absent")); };
            Value::Map { entries: values.into_iter().map(|(key, value)| Ok((Value::Utf8(key), convert(&map.types, value, names, unions, element, depth + 1)?))).collect::<Result<_>>()?, sorted: false }
        }
        Avro::Record(values) => {
            let Schema::Record(record) = schema else { return Err(error("avro-decode", "Record value lacks its schema")); };
            Value::Struct(values.into_iter().map(|(name, value)| {
                let field = record.lookup.get(&name).and_then(|index| record.fields.get(*index)).ok_or_else(|| error("avro-decode", "Unknown record field"))?;
                let child = match &connect.kind {
                    SchemaKind::Struct(fields) => fields.iter().find(|field| field.name == name).map(|field| &field.schema),
                    SchemaKind::Map(key, value) => match name.as_str() { "key" => Some(key), "value" => Some(value), _ => None },
                    _ => None,
                }.ok_or_else(|| error("avro-decode", "Record field Connect schema is absent"))?;
                Ok((name, convert(&field.schema, value, names, unions, child, depth + 1)?))
            }).collect::<Result<_>>()?)
        }
        Avro::Union(index, value) => {
            let Schema::Union(union) = schema else { return Err(error("avro-decode", "Union value lacks its schema")); };
            let selected = union.variants().get(index as usize).ok_or_else(|| error("avro-decode", "Invalid union member"))?;
            if matches!(value.as_ref(), Avro::Null) { Value::Null }
            else if (union.variants().len() == 1 && unions.flatten_singleton) || (union.variants().len() == 2 && union.variants().iter().any(|schema| matches!(schema, Schema::Null))) { convert(selected, *value, names, unions, &connect, depth + 1)? }
            else { union::convert(union.variants(), selected, *value, names, unions, &connect, depth + 1)? }
        }
        Avro::Duration(value) => Value::Binary(<[u8; 12]>::from(value).to_vec()),
        Avro::BigDecimal(_) => return Err(error("avro-decode", "Avro big-decimal extension requires its original byte representation")),
    })
}
fn decimal_value(bytes: Vec<u8>, scale: usize) -> Result<Value> {
    let coefficient = num_bigint::BigInt::from_signed_bytes_be(&bytes);
    let digits = coefficient.to_str_radix(10).trim_start_matches('-').len().max(1);
    let precision = if digits <= 32 { 32 } else if digits <= 64 { 64 } else { return Err(error("avro-decode", "Decimal precision exceeds the reference writer limit")); };
    let width = precision / 2; let bytes = coefficient.to_signed_bytes_be();
    if bytes.len() > width { return Err(error("avro-decode", "Decimal coefficient exceeds its width")); }
    let mut unscaled = vec![if coefficient.sign() == num_bigint::Sign::Minus { 255 } else { 0 }; width]; unscaled[width - bytes.len()..].copy_from_slice(&bytes);
    Ok(Value::Decimal(DecimalValue::new(unscaled, precision as u8, i8::try_from(scale).map_err(|cause| error("avro-decode", cause))?, precision as u16 * 4)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recursive_avro_values_and_detached_field_views_keep_schema_definitions() {
        let plan = AvroPlan::parse(r#"{"type":"record","name":"Node","fields":[{"name":"value","type":"long"},{"name":"next","type":["null","Node"],"default":null}]}"#, &[]).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        let Value::Struct(value) = plan.decode(&[2, 2, 4, 0], &mut context).unwrap().value else { panic!("expected Struct") };
        assert_eq!(value["value"], Value::I64(1));
        assert_eq!(value["next"], Value::Struct(BTreeMap::from([("value".into(), Value::I64(2)), ("next".into(), Value::Null)])));
        let next = plan.connect.field("next").unwrap().schema; drop(plan);
        assert!(next.optional); assert_eq!(next.field("value").unwrap().schema.resolved().kind, SchemaKind::I64);
    }
    #[test]
    fn enhanced_avro_unions_preserve_namespaces_for_equal_short_names() {
        let schema = r#"[{"type":"record","name":"a.Item","fields":[]},{"type":"record","name":"b.Item","fields":[]}]"#;
        assert!(AvroPlan::parse(schema, &[]).is_err());
        let plan = AvroPlan::parse_version(schema, &[], &BTreeMap::from([("enhanced.avro.schema.support".into(), "true".into())]), None).unwrap();
        assert!(plan.connect.field("a.Item").is_some()); assert!(plan.connect.field("b.Item").is_some());
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        assert_eq!(plan.decode(&[2], &mut context).unwrap().value, Value::Struct(BTreeMap::from([("a.Item".into(), Value::Null), ("b.Item".into(), Value::Struct(BTreeMap::new()))])));
    }
    #[test]
    fn generalized_avro_unions_count_null_positions_and_share_converter_numbering() {
        let options = BTreeMap::from([("generalized.sum.type.support".into(), "true".into()), ("flatten.singleton.unions".into(), "false".into())]);
        let mut index = 5;
        let first = AvroPlan::parse_version_index(r#"["null","int","string"]"#, &[], &options, None, &mut index).unwrap();
        assert_eq!(first.connect.name.as_deref(), Some("connect_union_5"));
        assert!(first.connect.field("connect_union_field_0").is_none());
        let second = AvroPlan::parse_version_index(r#"["long"]"#, &[], &options, None, &mut index).unwrap();
        assert_eq!(second.connect.name.as_deref(), Some("connect_union_6"));
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        assert_eq!(second.decode(&[0, 84], &mut context).unwrap().value, Value::Struct(BTreeMap::from([("connect_union_field_0".into(), Value::I64(42))])));
        let enumeration = AvroPlan::parse_version(r#"{"type":"enum","name":"Letter","symbols":["A","B"]}"#, &[], &options, None).unwrap();
        assert_eq!(enumeration.connect.parameters["org.apache.kafka.connect.data.Enum.B"], "1");
    }
    #[test]
    fn generalized_avro_null_positions_preserve_reference_field_lookup_failures() {
        let options = BTreeMap::from([("generalized.sum.type.support".into(), "true".into())]);
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        let before = AvroPlan::parse_version(r#"["null","int","string"]"#, &[], &options, None).unwrap();
        assert_eq!(before.decode(&[0], &mut context).unwrap().value, Value::Null);
        assert!(before.decode(&[2, 84], &mut context).is_err());
        assert!(before.decode(&[4, 2, b'x'], &mut context).is_err());
        let after = AvroPlan::parse_version(r#"["int","string","null"]"#, &[], &options, None).unwrap();
        assert_eq!(after.decode(&[0, 84], &mut context).unwrap().value, Value::Struct(BTreeMap::from([("connect_union_field_0".into(), Value::I32(42)), ("connect_union_field_1".into(), Value::Null)])));
        assert_eq!(after.decode(&[2, 2, b'x'], &mut context).unwrap().value, Value::Struct(BTreeMap::from([("connect_union_field_0".into(), Value::Null), ("connect_union_field_1".into(), Value::Utf8("x".into()))])));
        let pair = AvroPlan::parse_version(r#"["null","int"]"#, &[], &options, None).unwrap();
        assert_eq!(pair.decode(&[2, 84], &mut context).unwrap().value, Value::I32(42));
    }
    #[test]
    fn generalized_avro_null_positions_keep_compatible_destination_fields() {
        let options = BTreeMap::from([("generalized.sum.type.support".into(), "true".into())]);
        let schema = r#"["null",{"type":"enum","name":"Letter","symbols":["A"]},"string"]"#;
        let plan = AvroPlan::parse_version(schema, &[], &options, None).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        assert_eq!(plan.decode(&[4, 2, b'x'], &mut context).unwrap().value, Value::Struct(BTreeMap::from([("connect_union_field_1".into(), Value::Utf8("x".into())), ("connect_union_field_2".into(), Value::Null)])));
        let schema = r#"[{"type":"fixed","name":"A","size":1},"null",{"type":"fixed","name":"B","size":2}]"#;
        let plan = AvroPlan::parse_version(schema, &[], &options, None).unwrap();
        assert_eq!(plan.decode(&[4, 7, 8], &mut context).unwrap().value, Value::Struct(BTreeMap::from([("connect_union_field_0".into(), Value::Binary(vec![7, 8])), ("connect_union_field_2".into(), Value::Null)])));
    }
    #[test]
    fn enhanced_avro_fixed_unions_match_full_names_before_converting_values() {
        let schema = r#"[{"type":"fixed","name":"a.Value","size":1},{"type":"fixed","name":"b.Value","size":2}]"#;
        let plan = AvroPlan::parse_version(schema, &[], &BTreeMap::from([("enhanced.avro.schema.support".into(), "true".into())]), None).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        assert_eq!(plan.decode(&[2, 7, 8], &mut context).unwrap().value, Value::Struct(BTreeMap::from([("a.Value".into(), Value::Null), ("b.Value".into(), Value::Binary(vec![7, 8]))])));
    }
    #[test]
    fn avro_narrow_integer_unions_only_convert_when_flattened() {
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        for (annotation, value) in [("int8", Value::I8(1)), ("int16", Value::I16(1))] {
            let member = format!(r#"{{"type":"int","connect.type":"{annotation}"}}"#);
            let singleton = format!("[{member}]");
            assert_eq!(AvroPlan::parse(&singleton, &[]).unwrap().decode(&[0, 2], &mut context).unwrap().value, value);
            let nullable = format!("[\"null\",{member}]");
            assert_eq!(AvroPlan::parse(&nullable, &[]).unwrap().decode(&[2, 2], &mut context).unwrap().value, value);
            let multiple = format!("[{member},\"string\"]");
            assert!(AvroPlan::parse(&multiple, &[]).unwrap().decode(&[0, 2], &mut context).is_err());
            let options = BTreeMap::from([("flatten.singleton.unions".into(), "false".into())]);
            assert!(AvroPlan::parse_version(&singleton, &[], &options, None).unwrap().decode(&[0, 2], &mut context).is_err());
        }
    }
    #[test]
    fn avro_invalid_connect_defaults_are_ignored_after_nested_validation() {
        let record = AvroPlan::parse(r#"{"type":"record","name":"R","connect.default":{},"fields":[{"name":"required","type":"string"}]}"#, &[]).unwrap();
        assert!(record.connect.default.is_none());
        let array = AvroPlan::parse(r#"{"type":"array","items":"string","connect.default":[null]}"#, &[]).unwrap();
        assert!(array.connect.default.is_none());
        let optional = AvroPlan::parse(r#"{"type":"array","items":["null","string"],"connect.default":[null]}"#, &[]).unwrap();
        assert_eq!(optional.connect.default, Some(Value::List(vec![Value::Null])));
        let record = AvroPlan::parse(r#"{"type":"record","name":"R","connect.default":{},"fields":[{"name":"required","type":"string","default":"x"}]}"#, &[]).unwrap();
        assert!(record.connect.default.is_none());
        let nested = AvroPlan::parse(r#"{"type":"record","name":"R","fields":[{"name":"nested","type":{"type":"record","name":"N","fields":[{"name":"required","type":"string","default":"x"}]},"default":{}}]}"#, &[]).unwrap();
        assert_eq!(nested.connect.field("nested").unwrap().schema.default, Some(Value::Struct(BTreeMap::from([("required".into(), Value::Utf8("x".into()))]))));
    }
    #[test]
    fn nested_avro_records_nullable_union_and_logical_types_decode_with_bounded_preflight() {
        let schema = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"optional","type":["null","string"]},{"name":"day","type":{"type":"int","logicalType":"date"}},{"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}]}"#;
        let parsed = Schema::parse_str(schema).unwrap();
        let bytes = apache_avro::to_avro_datum(&parsed, Avro::Record(vec![
            ("id".into(), Avro::Long(7)), ("optional".into(), Avro::Union(1, Box::new(Avro::String("value".into())))),
            ("day".into(), Avro::Date(20_000)), ("amount".into(), Avro::Decimal(vec![0x30, 0x39].into())),
        ])).unwrap();
        let plan = AvroPlan::parse(schema, &[]).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        let Value::Struct(values) = plan.decode(&bytes, &mut context).unwrap().value else { panic!("expected Struct") };
        assert_eq!(values["id"], Value::I64(7)); assert_eq!(values["optional"], Value::Utf8("value".into())); assert_eq!(values["day"], Value::DateDay(20_000));
        let Value::Decimal(amount) = &values["amount"] else { panic!("expected Decimal") };
        assert_eq!(amount.precision, 32); assert_eq!(amount.scale, 2); assert_eq!(&amount.unscaled[14..], &[0x30, 0x39]);
    }
    #[test]
    fn confluent_root_bytes_have_no_avro_length_prefix() {
        let plan = AvroPlan::parse(r#""bytes""#, &[]).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 20));
        assert_eq!(plan.decode(&[255, 0, 3], &mut context).unwrap().value, Value::Binary(vec![255, 0, 3]));
    }
    #[test]
    fn avro_connect_annotations_defaults_and_registry_version_are_preserved() {
        let schema = r#"{"type":"record","name":"Event","doc":"avro doc","connect.doc":"connect doc","connect.version":8,"fields":[{"name":"small","type":{"type":"int","connect.type":"int8"},"default":257},{"name":"fixed","type":{"type":"fixed","name":"Bytes","size":2}},{"name":"choice","type":{"type":"enum","name":"Choice","symbols":["A","B"]}}]}"#;
        let plan = AvroPlan::parse_version(schema, &[], &BTreeMap::new(), Some(3)).unwrap();
        assert_eq!(plan.connect.version, Some(8)); assert_eq!(plan.connect.doc.as_deref(), Some("connect doc"));
        assert_eq!(plan.connect.parameters["io.confluent.connect.avro.record.doc"], "avro doc");
        assert_eq!(plan.connect.field("small").unwrap().schema.default, Some(Value::I8(1)));
        assert_eq!(plan.connect.field("fixed").unwrap().schema.parameters["connect.fixed.size"], "2");
        assert_eq!(plan.connect.field("choice").unwrap().schema.parameters["io.confluent.connect.avro.Enum.B"], "B");
        let parsed = Schema::parse_str(schema).unwrap();
        let bytes = apache_avro::to_avro_datum(&parsed, Avro::Record(vec![("small".into(), Avro::Int(257)), ("fixed".into(), Avro::Fixed(2, vec![0, 1])), ("choice".into(), Avro::Enum(1, "B".into()))])).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        let Value::Struct(values) = plan.decode(&bytes, &mut context).unwrap().value else { panic!("expected Struct") }; assert_eq!(values["small"], Value::I8(1));
    }
    #[test]
    fn avro_map_entry_arrays_become_non_string_key_maps() {
        let schema = r#"{"type":"array","items":{"type":"record","name":"MapEntry","namespace":"io.confluent.connect.avro","fields":[{"name":"key","type":"long"},{"name":"value","type":"string"}]}}"#;
        let parsed = Schema::parse_str(schema).unwrap();
        let bytes = apache_avro::to_avro_datum(&parsed, Avro::Array(vec![Avro::Record(vec![("key".into(), Avro::Long(9)), ("value".into(), Avro::String("nine".into()))])])).unwrap();
        let plan = AvroPlan::parse(schema, &[]).unwrap(); let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        assert_eq!(plan.decode(&bytes, &mut context).unwrap().value, Value::Map { entries: vec![(Value::I64(9), Value::Utf8("nine".into()))], sorted: false });
    }
    #[test]
    fn avro_collection_counts_are_checked_before_allocating_elements() {
        let schema = Schema::parse_str(r#"{"type":"array","items":"null"}"#).unwrap();
        let names = BTreeMap::new();
        let mut preflight = Preflight { bytes: &[0x80, 0x80, 0x80, 0x01, 0], offset: 0, allocated: 0, limit: 1 << 20, names: &names, view_bytes: 0 };
        assert!(preflight.value(&schema, 0).unwrap_err().code.ends_with("batch-too-large"));
    }
}
