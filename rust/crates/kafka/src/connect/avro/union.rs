use super::*;

#[cfg(test)]
pub(super) fn convert(variants: &[Schema], selected: &Schema, value: Avro, names: &BTreeMap<String, Schema>, options: graph::UnionOptions, connect: &ConnectSchema, depth: usize) -> Result<Value> {
    let SchemaKind::Struct(fields) = &connect.kind else { return Err(error("avro-decode", "Union Connect schema is not a Struct")); };
    let selected_index = variants.iter().position(|member| std::ptr::eq(member, selected)).ok_or_else(|| error("avro-decode", "Selected union branch is absent"))?;
    let selected = resolve(selected, names)?;
    let selected_name = options.field_name(selected, selected_index);
    let selected_connect = fields.iter().find(|field| field.name == selected_name).ok_or_else(|| error("avro-decode", "Selected union schema field is absent"))?;
    for (index, (field, member)) in fields.iter().zip(variants.iter().filter(|schema| !matches!(schema, Schema::Null))).enumerate() {
        let member = resolve(member, names)?;
        let candidate = field.schema.resolved();
        if !matches_member(&candidate, &selected_connect.schema, selected, &value, options, index)? { continue; }
        let destination_name = field_name(&candidate, options, index)?;
        let destination = fields.iter().find(|field| field.name == destination_name).ok_or_else(|| error("avro-decode", "Converted union field is absent from its schema"))?;
        let value = metadata::coerce(super::convert(member, value, names, options, &candidate, depth)?, &candidate)?;
        let value = into_destination(value, &candidate, &destination.schema, depth)?;
        let mut values = fields.iter().map(|field| (field.name.clone(), Value::Null)).collect::<StructFields<_>>();
        values.insert(destination_name, value);
        return Ok(Value::Struct(values));
    }
    Err(error("avro-decode", "No Connect union member matches the Avro value"))
}

pub(super) fn field_name(schema: &ConnectSchema, options: graph::UnionOptions, index: usize) -> Result<String> {
    if options.generalized { return Ok(format!("connect_union_field_{index}")); }
    if matches!(schema.kind, SchemaKind::Struct(_)) || enumeration(schema).is_some() || schema.parameters.contains_key("connect.fixed.size") {
        let name = schema.name.as_deref().ok_or_else(|| error("avro-decode", "Named union member lacks a Connect name"))?;
        return Ok(if options.enhanced { name } else { name.rsplit('.').next().unwrap_or(name) }.to_owned());
    }
    Ok(match schema.kind {
        SchemaKind::Bool => "boolean", SchemaKind::I32 => "int", SchemaKind::I64 => "long", SchemaKind::F32 => "float", SchemaKind::F64 => "double", SchemaKind::String => "string", SchemaKind::Bytes => "bytes", SchemaKind::Array(_) => "array", SchemaKind::Map(_, _) => "map",
        _ => return Err(error("avro-decode", "Connect union member has no Avro field name")),
    }.into())
}

fn enumeration(schema: &ConnectSchema) -> Option<&str> {
    schema.parameters.get("org.apache.kafka.connect.data.Enum").or_else(|| schema.parameters.get("io.confluent.connect.avro.Enum")).map(String::as_str)
}
#[derive(Clone, Copy)]
pub(super) enum Probe {
    Bool, Int, Long, Float, Double, Text, Binary(usize), Array, Map, Record, Other,
}
impl From<&Avro> for Probe {
    fn from(value: &Avro) -> Self {
        match value {
            Avro::Boolean(_) => Self::Bool,
            Avro::Int(_) => Self::Int,
            Avro::Long(_) => Self::Long,
            Avro::Float(_) => Self::Float,
            Avro::Double(_) => Self::Double,
            Avro::String(_) | Avro::Utf8(_) => Self::Text,
            Avro::Bytes(bytes) | Avro::Fixed(_, bytes) => Self::Binary(bytes.len()),
            Avro::Array(_) | Avro::PackedArray(_) => Self::Array,
            Avro::Map(_) => Self::Map,
            Avro::Record(_) => Self::Record,
            _ => Self::Other,
        }
    }
}

pub(super) struct Selected {
    enumeration: Option<String>,
    fixed: Option<String>,
}
impl Selected {
    pub(super) fn new(schema: &Schema, options: graph::UnionOptions) -> Self {
        let enumeration = match schema {
            Schema::Enum(enumeration) => Some(enumeration.name.fullname(None)),
            _ => None,
        };
        let physical = match schema { Schema::Decimal(decimal) => decimal.inner.as_ref(), other => other };
        let fixed = matches!(physical, Schema::Fixed(_)).then(|| options.field_name(schema, 0));
        Self { enumeration, fixed }
    }
}

pub(super) enum Matcher {
    Kind(Probe),
    Record,
    Binary { fixed: Option<FixedMatch> },
    Enum(String),
    Never,
}

pub(super) struct FixedMatch {
    size: Result<usize>,
    name: Result<String>,
    generalized: bool,
}

impl Matcher {
    pub(super) fn compile(candidate: &ConnectSchema, options: graph::UnionOptions, index: usize) -> Self {
        if let Some(name) = enumeration(candidate) {
            return Self::Enum(name.to_owned());
        }
        Self::Kind(match &candidate.kind {
            SchemaKind::I32 => Probe::Int,
            SchemaKind::I64 => Probe::Long,
            SchemaKind::F32 => Probe::Float,
            SchemaKind::F64 => Probe::Double,
            SchemaKind::Bool => Probe::Bool,
            SchemaKind::String => Probe::Text,
            SchemaKind::Array(_) => Probe::Array,
            SchemaKind::Map(_, _) => Probe::Map,
            SchemaKind::Struct(_) => return Self::Record,
            SchemaKind::Bytes => return Self::Binary {
                fixed: candidate.parameters.get("connect.fixed.size").map(|size| FixedMatch {
                    size: size.parse().map_err(|cause| error("avro-decode", cause)),
                    name: field_name(candidate, options, index),
                    generalized: options.generalized,
                }),
            },
            _ => return Self::Never,
        })
    }

    pub(super) fn matches(&self, probe: Probe, candidate: &ConnectSchema, selected: &Selected, selected_connect: &ConnectSchema) -> Result<bool> {
        match (self, probe) {
            (Self::Enum(expected), _) => Ok(selected.enumeration.as_ref() == Some(expected)),
            (Self::Kind(expected), probe) => Ok(std::mem::discriminant(expected) == std::mem::discriminant(&probe)),
            (Self::Record, Probe::Record) => same_schema(&candidate.resolved(), selected_connect, &mut Default::default(), 0),
            (Self::Binary { fixed: None }, Probe::Binary(_)) => Ok(true),
            (Self::Binary { fixed: Some(fixed) }, Probe::Binary(size)) => {
                if let Some(selected) = &selected.fixed {
                    let name = fixed.name.as_ref().map_err(Clone::clone)?;
                    Ok(fixed.generalized || selected == name)
                } else {
                    Ok(*fixed.size.as_ref().map_err(Clone::clone)? == size)
                }
            }
            _ => Ok(false),
        }
    }
}

#[cfg(test)]
pub(super) fn matches_member(candidate: &ConnectSchema, selected_connect: &ConnectSchema, selected: &Schema, value: &Avro, options: graph::UnionOptions, index: usize) -> Result<bool> {
    if let Some(name) = enumeration(candidate) { return Ok(matches!(selected, Schema::Enum(schema) if schema.name.fullname(None) == name)); }
    Ok(match &candidate.kind {
        SchemaKind::I32 => matches!(value, Avro::Int(_)),
        SchemaKind::I64 => matches!(value, Avro::Long(_)),
        SchemaKind::F32 => matches!(value, Avro::Float(_)), SchemaKind::F64 => matches!(value, Avro::Double(_)),
        SchemaKind::Bool => matches!(value, Avro::Boolean(_)), SchemaKind::String => matches!(value, Avro::String(_) | Avro::Utf8(_)),
        SchemaKind::Array(_) => matches!(value, Avro::Array(_) | Avro::PackedArray(_)), SchemaKind::Map(_, _) => matches!(value, Avro::Map(_)),
        SchemaKind::Struct(_) => matches!(value, Avro::Record(_)) && same_schema(candidate, selected_connect, &mut Default::default(), 0)?,
        SchemaKind::Bytes => {
            if !matches!(value, Avro::Bytes(_) | Avro::Fixed(_, _)) { false }
            else if let Some(size) = candidate.parameters.get("connect.fixed.size") {
                let selected_physical = match selected { Schema::Decimal(decimal) => decimal.inner.as_ref(), other => other };
                if matches!(selected_physical, Schema::Fixed(_)) {
                    options.field_name(selected, index) == field_name(candidate, options, index)?
                } else {
                    let size = size.parse::<usize>().map_err(|cause| error("avro-decode", cause))?;
                    match value {
                        Avro::Bytes(bytes) | Avro::Fixed(_, bytes) => bytes.len() == size,
                        _ => false,
                    }
                }
            } else { true }
        }
        _ => false,
    })
}


pub(super) fn validate_destination(value: &Value, source: &ConnectSchema, destination: &ConnectSchema, depth: usize) -> Result<()> {
    if depth > 128 { return Err(error("avro-decode", "Connect union validation nesting limit exceeded")); }
    let source = source.resolved();
    let destination = destination.resolved();
    if *value == Value::Null {
        return if destination.optional { Ok(()) } else { Err(error("avro-decode", "Null union value targets a required field")) };
    }
    match destination.name.as_deref() {
        Some("org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time" | "org.apache.kafka.connect.data.Timestamp") => {
            return super::super::data::date_millis(value).map(drop).ok_or_else(|| error("avro-decode", "Union field requires a Connect date value"));
        }
        Some("org.apache.kafka.connect.data.Decimal") => {
            return if matches!(value, Value::Decimal(_)) { Ok(()) } else { Err(error("avro-decode", "Union field requires a Connect decimal value")) };
        }
        _ => {}
    }
    match (value, &source.kind, &destination.kind) {
        (Value::Bool(_), _, SchemaKind::Bool) | (Value::I8(_), _, SchemaKind::I8)
        | (Value::I16(_), _, SchemaKind::I16) | (Value::I32(_), _, SchemaKind::I32)
        | (Value::I64(_), _, SchemaKind::I64) | (Value::F32(_), _, SchemaKind::F32)
        | (Value::F64(_), _, SchemaKind::F64) | (Value::Utf8(_), _, SchemaKind::String)
        | (Value::Binary(_), _, SchemaKind::Bytes) => Ok(()),
        (Value::List(values), SchemaKind::Array(_), SchemaKind::Array(destination))
            if depth < 128 && values.array().is_some() && numeric_array_unchanged(values, destination) => Ok(()),
        (Value::List(values), SchemaKind::Array(source), SchemaKind::Array(destination)) => {
            values.iter().try_for_each(|value| validate_destination(value.as_ref(), source, destination, depth + 1))
        }
        (Value::Map { entries, .. }, SchemaKind::Map(source_key, source_value), SchemaKind::Map(destination_key, destination_value)) => {
            entries.iter().try_for_each(|(key, value)| {
                validate_destination(key, source_key, destination_key, depth + 1)?;
                validate_destination(value, source_value, destination_value, depth + 1)
            })
        }
        (Value::Struct(_), SchemaKind::Struct(_), SchemaKind::Struct(_))
            if same_schema(&source, &destination, &mut Default::default(), depth)? => Ok(()),
        _ => Err(error("avro-decode", "Converted union value does not match its destination field")),
    }
}

#[cfg(test)]
pub(super) fn into_destination(value: Value, source: &ConnectSchema, destination: &ConnectSchema, depth: usize) -> Result<Value> {
    if depth > 128 { return Err(error("avro-decode", "Connect union validation nesting limit exceeded")); }
    let source = source.resolved(); let destination = destination.resolved();
    if value == Value::Null { return if destination.optional { Ok(value) } else { Err(error("avro-decode", "Null union value targets a required field")) }; }
    if matches!(destination.name.as_deref(), Some("org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time" | "org.apache.kafka.connect.data.Timestamp")) {
        let millis = super::super::data::date_millis(&value).ok_or_else(|| error("avro-decode", "Union field requires a Connect date value"))?;
        return Ok(super::super::data::date_value(millis));
    }
    if destination.name.as_deref() == Some("org.apache.kafka.connect.data.Decimal") {
        return if matches!(value, Value::Decimal(_)) { Ok(value) } else { Err(error("avro-decode", "Union field requires a Connect decimal value")) };
    }
    Ok(match (value, &source.kind, &destination.kind) {
        (value @ Value::Bool(_), _, SchemaKind::Bool) | (value @ Value::I8(_), _, SchemaKind::I8) | (value @ Value::I16(_), _, SchemaKind::I16) | (value @ Value::I32(_), _, SchemaKind::I32) | (value @ Value::I64(_), _, SchemaKind::I64) | (value @ Value::F32(_), _, SchemaKind::F32) | (value @ Value::F64(_), _, SchemaKind::F64) | (value @ Value::Utf8(_), _, SchemaKind::String) | (value @ Value::Binary(_), _, SchemaKind::Bytes) => value,
        (Value::List(values), SchemaKind::Array(_), SchemaKind::Array(destination))
            if depth < 128 && values.array().is_some() && numeric_array_unchanged(&values, destination) => Value::List(values),
        (Value::List(values), SchemaKind::Array(source), SchemaKind::Array(destination)) => Value::list(values.into_values().into_iter().map(|value| into_destination(value, source, destination, depth + 1)).collect::<Result<_>>()?),
        (Value::Map { entries, sorted }, SchemaKind::Map(source_key, source_value), SchemaKind::Map(destination_key, destination_value)) => Value::Map { entries: entries.into_iter().map(|(key, value)| Ok((into_destination(key, source_key, destination_key, depth + 1)?, into_destination(value, source_value, destination_value, depth + 1)?))).collect::<Result<_>>()?, sorted },
        (value @ Value::Struct(_), SchemaKind::Struct(_), SchemaKind::Struct(_)) if same_schema(&source, &destination, &mut Default::default(), depth)? => value,
        _ => return Err(error("avro-decode", "Converted union value does not match its destination field")),
    })
}

fn numeric_array_unchanged(list: &rtdb_types::ListValue, destination: &ConnectSchema) -> bool {
    use rtdb_types::{LogicalType, ScalarType};
    let destination = destination.resolved();
    if matches!(destination.name.as_deref(), Some("org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time"
        | "org.apache.kafka.connect.data.Timestamp" | "org.apache.kafka.connect.data.Decimal")) {
        return false;
    }
    let scalar = match destination.kind {
        SchemaKind::I8 => ScalarType::I8,
        SchemaKind::I16 => ScalarType::I16,
        SchemaKind::I32 => ScalarType::I32,
        SchemaKind::I64 => ScalarType::I64,
        SchemaKind::F32 => ScalarType::F32,
        SchemaKind::F64 => ScalarType::F64,
        _ => return false,
    };
    let expected = LogicalType::Scalar(scalar);
    list.element_type().legs().into_iter().all(|leg| leg == expected || (leg == LogicalType::Null && destination.optional))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_array_with_a_connect_name_keeps_child_buffers_in_a_generalized_union() {
        let options = BTreeMap::from([("generalized.sum.type.support".into(), "true".into())]);
        let plan = AvroPlan::parse_version(
            r#"[{"type":"array","items":{"type":"float","connect.name":"Magnitude"}},"string","null"]"#,
            &[], &options, None,
        ).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        let mut bytes = vec![0, 4];
        bytes.extend_from_slice(&(-0.0_f32).to_le_bytes());
        bytes.extend_from_slice(&0x7fc1_2345_u32.to_le_bytes());
        bytes.push(0);
        let datum = plan.datum.decode_datum(&bytes, &mut context).unwrap();
        let Avro::Union(_, inner) = &datum.value else { panic!("expected union datum"); };
        let Avro::PackedArray(packed) = inner.as_ref() else { panic!("expected packed array"); };
        let (_, input) = packed.values.packed_column().unwrap();

        let Value::Struct(fields) = plan.convert(datum, &mut 0, &mut context).unwrap().value else { panic!("expected Connect union"); };
        assert_eq!(fields["connect_union_field_1"], Value::Null);
        let Value::List(list) = &fields["connect_union_field_0"] else { panic!("expected array member"); };
        let (_, output) = list.packed_column().unwrap();
        let child_buffer = |array: &arrow_array::ArrayRef| array.as_any().downcast_ref::<arrow_array::Float32Array>().unwrap().values().as_ptr();
        assert_eq!(child_buffer(&input), child_buffer(&output));
    }

    #[test]
    fn logical_union_destination_preserves_the_date_object_before_schema_admission() {
        let mut source = ConnectSchema::new(SchemaKind::I64);
        source.name = Some("org.apache.kafka.connect.data.Timestamp".into());
        for logical in ["Date", "Time", "Timestamp"] {
            let mut destination = ConnectSchema::new(if logical == "Timestamp" { SchemaKind::I64 } else { SchemaKind::I32 });
            destination.name = Some(format!("org.apache.kafka.connect.data.{logical}"));
            for millis in [i64::MIN, -1, 86_400_000, i64::MAX] {
                let value = super::super::super::data::date_value(millis);
                assert_eq!(into_destination(value.clone(), &source, &destination, 0).unwrap(), value);
            }
            assert!(into_destination(Value::I64(0), &source, &destination, 0).is_err());
        }
    }
}

fn same_schema(left: &ConnectSchema, right: &ConnectSchema, visited: &mut std::collections::BTreeSet<((usize, usize), (usize, usize))>, depth: usize) -> Result<bool> {
    if left == right { return Ok(true); }
    if let (SchemaKind::Reference(left), SchemaKind::Reference(right)) = (&left.kind, &right.kind) {
        if !visited.insert((left.identity(), right.identity())) { return Ok(true); }
    }
    if depth > 128 { return Err(error("avro-decode", "Connect schema comparison nesting limit exceeded")); }
    let left = left.resolved(); let right = right.resolved();
    if left.optional != right.optional || left.name != right.name || left.version != right.version || left.doc != right.doc || left.parameters != right.parameters || !super::super::data::defaults_equal(left.default_value(), right.default_value()) { return Ok(false); }
    Ok(match (&left.kind, &right.kind) {
        (SchemaKind::Struct(left), SchemaKind::Struct(right)) => {
            if left.len() != right.len() { return Ok(false); }
            for (left, right) in left.iter().zip(right) { if left.name != right.name || !same_schema(&left.schema, &right.schema, visited, depth + 1)? { return Ok(false); } }
            true
        }
        (SchemaKind::Array(left), SchemaKind::Array(right)) => same_schema(left, right, visited, depth + 1)?,
        (SchemaKind::Map(left_key, left_value), SchemaKind::Map(right_key, right_value)) => same_schema(left_key, right_key, visited, depth + 1)? && same_schema(left_value, right_value, visited, depth + 1)?,
        (left, right) => left == right,
    })
}
