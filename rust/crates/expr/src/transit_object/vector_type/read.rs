use super::*;
use super::arrow::{ArrowKind,Extension,Unit};

pub(super) fn read(spec: &Object,scope: &EvaluationScope) -> Result<Arc<Type>> {
    match spec {
        Object::VectorType(value) => Ok(Arc::clone(value)),
        Object::Keyword(name) => match name.as_str() {
            "null" => retained(Type::Null,scope),"nothing" => retained(Type::Nothing,scope),"tstz-range" => range(scope),
            _ => scalar(ArrowKind::read(spec,scope)?,scope),
        },
        Object::Set(values) => from_set(values,scope),Object::Vector(values) => read_vector(values,scope),
        _ => {
            let wire = spec.wire(scope)?;
            scope.reserve(rtdb_format::transit::json_value_allocation_bound(&wire)?.checked_add(1024).ok_or_else(overflow)?)?;
            Err(Anomaly::incorrect("invalid-type", "Invalid type").with_context("type-spec",rtdb_format::transit::to_json_value(&wire)?))
        }
    }
}
fn scalar(arrow: ArrowKind,scope: &EvaluationScope) -> Result<Arc<Type>> { retained(Type::Mono(retained(Mono::Scalar { arrow: retained(arrow,scope)?,encoding: Encoding::Plain },scope)?),scope) }
fn range(scope: &EvaluationScope) -> Result<Arc<Type>> {
    let instant = scalar(ArrowKind::Timestamp { unit: Unit::Micro,timezone: Some(text_arc("UTC",scope)?) },scope)?;
    retained(Type::Mono(retained(Mono::Listy { arrow: retained(ArrowKind::Extension(Extension::TsTzRange),scope)?,element: instant,encoding: Encoding::Plain },scope)?),scope)
}
fn read_vector(values: &[Object],scope: &EvaluationScope) -> Result<Arc<Type>> {
    let nullable = values.iter().take_while(|value|matches!(value,Object::Keyword(name) if name == "?")).count();
    let mut value = read_vector_base(&values[nullable..],scope)?;
    for _ in 0..nullable { value = Type::maybe(&value,scope)?; }
    Ok(value)
}
fn read_vector_base(values: &[Object],scope: &EvaluationScope) -> Result<Arc<Type>> {
    let head = values.first().unwrap_or(&Object::Null);let tail = values.get(1..).unwrap_or(&[]);
    let Object::Keyword(name) = head else { return Err(invalid()); };
    match name.as_str() {
        "null" => retained(Type::Null,scope),"tstz-range" => range(scope),
        "struct" => fields(tail.first().unwrap_or(&Object::Null),scope),
        "ree" | "ree-i16" | "ree-i64" => {
            let child = read(tail.first().unwrap_or(&Object::Null),scope)?;
            let width = match name.as_str() { "ree-i16" => RunEndWidth::I16,"ree-i64" => RunEndWidth::I64,_ => RunEndWidth::I32 };
            Type::with_encoding(&child,Encoding::RunEnd(width),scope)
        }
        "map" => {
            let options = if tail.len() > 1 { &tail[0] } else { &Object::Null };
            let arrow = ArrowKind::map(options,scope)?;
            let element = read(tail.last().unwrap_or(&Object::Null),scope)?;
            retained(Type::Mono(retained(Mono::Listy { arrow: retained(arrow,scope)?,element,encoding: Encoding::Plain },scope)?),scope)
        }
        "list" | "set" | "fixed-size-list" => {
            let arrow = ArrowKind::read(head,scope)?;
            let element = read(tail.first().unwrap_or(&Object::Null),scope)?;
            retained(Type::Mono(retained(Mono::Listy { arrow: retained(arrow,scope)?,element,encoding: Encoding::Plain },scope)?),scope)
        }
        _ => scalar(ArrowKind::sequence(values,scope)?,scope),
    }
}
fn from_set(source: &Map,scope: &EvaluationScope) -> Result<Arc<Type>> {
    let mut converted = Map::set();
    for (spec,_) in source.entries() { let value = read(spec,scope)?;assoc(&mut converted,Object::VectorType(value),Object::Null,scope)?; }
    let mut legs = Legs::new();
    for (value,_) in converted.entries() {
        let Object::VectorType(value) = value else { unreachable!() };
        let leg = match value.as_ref() { Type::Null => Leg::Null,Type::Mono(mono) => Leg::Mono(Mono::unencoded(mono,scope)?),_ => return Err(invalid()) };
        legs.insert(leg,scope)?;
    }
    let mut nonnull = legs.ordered.iter().filter_map(|leg|match leg { Leg::Null => None,Leg::Mono(value) => Some(value) });
    let Some(single) = nonnull.next() else { return retained(Type::Null,scope); };
    if nonnull.next().is_none() {
        return retained(if legs.ordered.len() == 1 { Type::Mono(Arc::clone(single)) } else { Type::Maybe(Arc::clone(single)) },scope);
    }
    retained(Type::Poly { legs: retained(legs,scope)?,encoding: Encoding::Plain },scope)
}
fn fields(source: &Object,scope: &EvaluationScope) -> Result<Arc<Type>> {
    let mut fields = Map::new();
    let mut add = |key: &Object,value: &Object| -> Result<()> {
        let name = super::super::render::string(key,scope)?;
        let value = read(value,scope)?;
        assoc(&mut fields,Object::String(name),Object::VectorType(value),scope)
    };
    match source {
        Object::Map(map) => for (key,value) in map.entries() { add(key,value)?; },
        Object::TxKey(record) => for (key,value) in record.entries() { add(key,value)?; },
        Object::Vector(values) | Object::List(values) => for value in values { pair(value,scope,&mut add)?; },
        Object::Set(values) => for (value,_) in values.entries() { pair(value,scope,&mut add)?; },
        Object::Null => {},Object::String(value) if value.is_empty() => {},Object::Bytes(value) if value.is_empty() => {},
        Object::Path(super::super::Path::Unix(value)) if value.as_slice() == b"/" => {},
        _ => return Err(invalid()),
    }
    slots(fields.len(),scope)?;let mut ordered = Vec::with_capacity(fields.len());let mut values = BTreeMap::new();
    for (name,value) in fields.entries() {
        let (Object::String(name),Object::VectorType(value)) = (name,value) else { unreachable!() };
        let name = text_arc(name,scope)?;slots(1,scope)?;values.insert(Arc::clone(&name),Arc::clone(value));ordered.push(name);
    }
    slots(ordered.len(),scope)?;
    let fields = retained(Fields { ordered: Arc::from(ordered),values },scope)?;
    retained(Type::Mono(retained(Mono::Struct { fields,encoding: Encoding::Plain },scope)?),scope)
}
fn pair(value: &Object,scope: &EvaluationScope,mut add: impl FnMut(&Object,&Object)->Result<()>) -> Result<()> {
    match value {
        Object::Null => add(&Object::Null,&Object::Null),
        Object::Vector(values) | Object::List(values) => add(values.first().unwrap_or(&Object::Null),values.get(1).unwrap_or(&Object::Null)),
        Object::String(value) => {
            let mut units = value.encode_utf16();let key = units.next().map_or(Object::Null,Object::Character);let value = units.next().map_or(Object::Null,Object::Character);add(&key,&value)
        }
        Object::Bytes(value) => {
            let byte = |value: Option<&u8>| -> Result<Object> { match value { Some(value) => Ok(Object::Number(retained(super::super::Number::Byte(*value as i8),scope)?)),None => Ok(Object::Null) } };
            let key = byte(value.first())?;let value = byte(value.get(1))?;add(&key,&value)
        }
        _ => Err(invalid()),
    }
}
