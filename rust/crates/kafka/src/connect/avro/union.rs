use super::*;

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

fn field_name(schema: &ConnectSchema, options: graph::UnionOptions, index: usize) -> Result<String> {
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
pub(super) fn matches_member(candidate: &ConnectSchema, selected_connect: &ConnectSchema, selected: &Schema, value: &Avro, options: graph::UnionOptions, index: usize) -> Result<bool> {
    if let Some(name) = enumeration(candidate) { return Ok(matches!(selected, Schema::Enum(schema) if schema.name.fullname(None) == name)); }
    Ok(match &candidate.kind {
        SchemaKind::I32 => matches!(value, Avro::Int(_)),
        SchemaKind::I64 => matches!(value, Avro::Long(_)),
        SchemaKind::F32 => matches!(value, Avro::Float(_)), SchemaKind::F64 => matches!(value, Avro::Double(_)),
        SchemaKind::Bool => matches!(value, Avro::Boolean(_)), SchemaKind::String => matches!(value, Avro::String(_) | Avro::Utf8(_)),
        SchemaKind::Array(_) => matches!(value, Avro::Array(_)), SchemaKind::Map(_, _) => matches!(value, Avro::Map(_)),
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

fn into_destination(value: Value, source: &ConnectSchema, destination: &ConnectSchema, depth: usize) -> Result<Value> {
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
        (Value::List(values), SchemaKind::Array(source), SchemaKind::Array(destination)) => Value::list(values.into_values().into_iter().map(|value| into_destination(value, source, destination, depth + 1)).collect::<Result<_>>()?),
        (Value::Map { entries, sorted }, SchemaKind::Map(source_key, source_value), SchemaKind::Map(destination_key, destination_value)) => Value::Map { entries: entries.into_iter().map(|(key, value)| Ok((into_destination(key, source_key, destination_key, depth + 1)?, into_destination(value, source_value, destination_value, depth + 1)?))).collect::<Result<_>>()?, sorted },
        (value @ Value::Struct(_), SchemaKind::Struct(_), SchemaKind::Struct(_)) if same_schema(&source, &destination, &mut Default::default(), depth)? => value,
        _ => return Err(error("avro-decode", "Converted union value does not match its destination field")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
