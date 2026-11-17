use super::*;
use super::super::default_input::DefaultAttempt;

#[derive(Clone, Copy)]
struct Pair { left: (RegionId, usize), right: (RegionId, usize), equal: bool }
#[derive(Clone, Copy)]
enum ValueRelation { Ordinary, Deep }

fn depth_guard(depth: usize) -> Result<()> { if depth > 128 { Err(invalid("Schema-object comparison depth exceeded")) } else { Ok(()) } }

pub(super) fn ordinary(left: ObjectView<'_>, right: ObjectView<'_>, context: &mut ConversionContext, depth: usize) -> Result<bool> {
    let left = left.actual()?; let right = right.actual()?;
    if left.identity()? == right.identity()? { return Ok(true); }
    depth_guard(depth)?;
    match (&left.arena.objects[left.object.index], &right.arena.objects[right.object.index]) {
        (Object::Builder { .. }, Object::Builder { .. }) => Ok(false),
        (Object::Built(_), Object::Built(_)) => bodies(left.body()?, right.body()?, &mut Vec::new(), context, depth, false),
        (Object::Wrapper { target: left_target, optional: left_optional }, Object::Wrapper { target: right_target, optional: right_optional }) => {
            Ok(left_optional == right_optional && ordinary(left.child(*left_target), right.child(*right_target), context, depth + 1)?)
        }
        _ => Ok(false),
    }
}

pub(super) fn properties(left: ObjectView<'_>, right: ObjectView<'_>, context: &mut ConversionContext) -> Result<bool> { structural(left, right, &mut Vec::new(), context, 0) }
fn structural(left: ObjectView<'_>, right: ObjectView<'_>, cache: &mut Vec<Pair>, context: &mut ConversionContext, depth: usize) -> Result<bool> {
    let pair = (left.identity()?, right.identity()?);
    if pair.0 == pair.1 { return Ok(true); }
    if let Some(entry) = cache.iter().find(|entry| entry.left == pair.0 && entry.right == pair.1) { return Ok(entry.equal); }
    depth_guard(depth)?;
    let next = capacity(cache)?;
    if let Some((_, bytes)) = next { context.reserve(bytes)?; }
    grow(cache, next)?;
    let index = cache.len(); cache.push(Pair { left: pair.0, right: pair.1, equal: true });
    let equal = bodies(left.body()?, right.body()?, cache, context, depth, true)?;
    cache[index].equal = equal; Ok(equal)
}
fn child_equal(left: ObjectView<'_>, right: ObjectView<'_>, cache: &mut Vec<Pair>, context: &mut ConversionContext, depth: usize, structural_relation: bool) -> Result<bool> {
    if structural_relation { structural(left, right, cache, context, depth) } else { ordinary(left, right, context, depth) }
}
fn bodies(left: BodyView<'_>, right: BodyView<'_>, cache: &mut Vec<Pair>, context: &mut ConversionContext, depth: usize, structural_relation: bool) -> Result<bool> {
    let left_meta = &left.body.metadata; let right_meta = &right.body.metadata;
    if left.optional != right.optional || left_meta.version != right_meta.version || left_meta.name != right_meta.name || left_meta.doc != right_meta.doc { return Ok(false); }
    let kind_matches = match (&left.body.kind, &right.body.kind) {
        (Kind::Scalar(left), Kind::Scalar(right)) => left == right,
        (Kind::Array(_), Kind::Array(_)) | (Kind::Map(_, _), Kind::Map(_, _)) | (Kind::Struct(_), Kind::Struct(_)) => true,
        _ => false,
    };
    if !kind_matches || !default_equal(left_meta.default.as_ref(), left.arena, right_meta.default.as_ref(), right.arena, context, depth + 1)? { return Ok(false); }
    if structural_relation && left_meta.parameters != right_meta.parameters { return Ok(false); }
    let result = match (&left.body.kind, &right.body.kind) {
        (Kind::Struct(left_fields), Kind::Struct(right_fields)) => {
            if left_fields.len() != right_fields.len() { return Ok(false); }
            for (left_field, right_field) in left_fields.iter().zip(right_fields) {
                if left_field.name != right_field.name || !child_equal(ObjectView { arena: left.arena, object: left_field.schema }, ObjectView { arena: right.arena, object: right_field.schema }, cache, context, depth + 1, structural_relation)? { return Ok(false); }
            }
            true
        }
        (Kind::Array(left_child), Kind::Array(right_child)) => child_equal(ObjectView { arena: left.arena, object: *left_child }, ObjectView { arena: right.arena, object: *right_child }, cache, context, depth + 1, structural_relation)?,
        (Kind::Map(left_key, left_value), Kind::Map(right_key, right_value)) => {
            let left_key = ObjectView { arena: left.arena, object: *left_key }; let right_key = ObjectView { arena: right.arena, object: *right_key };
            let left_value = ObjectView { arena: left.arena, object: *left_value }; let right_value = ObjectView { arena: right.arena, object: *right_value };
            if structural_relation { child_equal(left_value, right_value, cache, context, depth + 1, true)? && child_equal(left_key, right_key, cache, context, depth + 1, true)? }
            else { child_equal(left_key, right_key, cache, context, depth + 1, false)? && child_equal(left_value, right_value, cache, context, depth + 1, false)? }
        }
        (Kind::Scalar(_), Kind::Scalar(_)) => true,
        _ => false,
    };
    Ok(result && (structural_relation || left_meta.parameters == right_meta.parameters))
}
fn default_equal(left: Option<&JavaValue>, left_arena: &SchemaObjects, right: Option<&JavaValue>, right_arena: &SchemaObjects, context: &mut ConversionContext, depth: usize) -> Result<bool> {
    match (left, right) { (None, None) => Ok(true), (Some(left), Some(right)) => values(left, left_arena, right, right_arena, context, depth, ValueRelation::Deep), _ => Ok(false) }
}
fn significant(mut bytes: &[u8]) -> &[u8] {
    while bytes.len() > 1 && ((bytes[0] == 0 && bytes[1] & 128 == 0) || (bytes[0] == 255 && bytes[1] & 128 != 0)) { bytes = &bytes[1..]; }
    bytes
}
fn values(left: &JavaValue, left_arena: &SchemaObjects, right: &JavaValue, right_arena: &SchemaObjects, context: &mut ConversionContext, depth: usize, relation: ValueRelation) -> Result<bool> {
    depth_guard(depth)?;
    use JavaValue as V;
    Ok(match (left, right) {
        (V::Null, V::Null) => true, (V::Bool(left), V::Bool(right)) => left == right,
        (V::I8(left), V::I8(right)) => left == right, (V::I16(left), V::I16(right)) => left == right,
        (V::I32(left), V::I32(right)) => left == right, (V::I64(left), V::I64(right)) => left == right,
        (V::F32(left), V::F32(right)) => left.to_bits() == right.to_bits() || left.is_nan() && right.is_nan(),
        (V::F64(left), V::F64(right)) => left.to_bits() == right.to_bits() || left.is_nan() && right.is_nan(),
        (V::String(left), V::String(right)) => left == right,
        (V::ByteArray(left), V::ByteArray(right)) => match relation { ValueRelation::Ordinary => Arc::ptr_eq(left, right), ValueRelation::Deep => left.bytes() == right.bytes() },
        (V::ByteBuffer(left), V::ByteBuffer(right)) => left.remaining() == right.remaining(),
        (V::Date(left), V::Date(right)) => left == right,
        (V::Decimal(left), V::Decimal(right)) => left.scale == right.scale && significant(&left.coefficient) == significant(&right.coefficient),
        (V::Array(left), V::Array(right)) => list_equal(left, left_arena, right, right_arena, context, depth + 1, ValueRelation::Ordinary)?,
        (V::Struct { schema: left_schema, values: left }, V::Struct { schema: right_schema, values: right }) => {
            ordinary(ObjectView { arena: left_arena, object: *left_schema }, ObjectView { arena: right_arena, object: *right_schema }, context, depth + 1)? && list_equal(left, left_arena, right, right_arena, context, depth + 1, ValueRelation::Deep)?
        }
        (V::Map(left), V::Map(right)) => {
            if left.len() != right.len() { return Ok(false); }
            for (left_key, left_value) in left {
                let mut found = false;
                for (right_key, right_value) in right {
                    if values(left_key, left_arena, right_key, right_arena, context, depth + 1, ValueRelation::Ordinary)? {
                        if !values(left_value, left_arena, right_value, right_arena, context, depth + 1, ValueRelation::Ordinary)? { return Ok(false); }
                        found = true; break;
                    }
                }
                if !found { return Ok(false); }
            }
            true
        }
        _ => false,
    })
}
fn list_equal(left: &[JavaValue], left_arena: &SchemaObjects, right: &[JavaValue], right_arena: &SchemaObjects, context: &mut ConversionContext, depth: usize, relation: ValueRelation) -> Result<bool> {
    if left.len() != right.len() { return Ok(false); }
    for (left, right) in left.iter().zip(right) { if !values(left, left_arena, right, right_arena, context, depth, relation)? { return Ok(false); } }
    Ok(true)
}

fn bad_class() -> DefaultAttempt<()> { DefaultAttempt::Invalid(error("avro-schema", "Typed default does not match its Java schema object")) }
pub(super) fn validate(value: &JavaValue, owner: &SchemaObjects, destination: ObjectView<'_>, context: &mut ConversionContext, depth: usize) -> Result<DefaultAttempt<()>> {
    depth_guard(depth)?;
    let schema = destination.body()?;
    if matches!(value, JavaValue::Null) { return Ok(if schema.optional { DefaultAttempt::Value(()) } else { bad_class() }); }
    let class_matches = match schema.body.metadata.name.as_deref() {
        Some("org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time" | "org.apache.kafka.connect.data.Timestamp") => matches!(value, JavaValue::Date(_)),
        Some("org.apache.kafka.connect.data.Decimal") => matches!(value, JavaValue::Decimal(_)),
        _ => match (&schema.body.kind, value) {
            (Kind::Scalar(Scalar::Bool), JavaValue::Bool(_)) | (Kind::Scalar(Scalar::I8), JavaValue::I8(_)) | (Kind::Scalar(Scalar::I16), JavaValue::I16(_)) | (Kind::Scalar(Scalar::I32), JavaValue::I32(_)) | (Kind::Scalar(Scalar::I64), JavaValue::I64(_)) | (Kind::Scalar(Scalar::F32), JavaValue::F32(_)) | (Kind::Scalar(Scalar::F64), JavaValue::F64(_)) | (Kind::Scalar(Scalar::String), JavaValue::String(_)) | (Kind::Scalar(Scalar::Bytes), JavaValue::ByteArray(_) | JavaValue::ByteBuffer(_)) | (Kind::Array(_), JavaValue::Array(_)) | (Kind::Map(_, _), JavaValue::Map(_)) | (Kind::Struct(_), JavaValue::Struct { .. }) => true,
            _ => false,
        },
    };
    if !class_matches { return Ok(bad_class()); }
    macro_rules! child { ($value:expr, $schema:expr, $owner:expr) => { if let DefaultAttempt::Invalid(cause) = validate($value, $owner, $schema, context, depth + 1)? { return Ok(DefaultAttempt::Invalid(cause)); } }; }
    match (&schema.body.kind, value) {
        (Kind::Array(element), JavaValue::Array(values)) => for value in values { child!(value, ObjectView { arena: schema.arena, object: *element }, owner); },
        (Kind::Map(key, element), JavaValue::Map(entries)) => for (entry_key, value) in entries { child!(entry_key, ObjectView { arena: schema.arena, object: *key }, owner); child!(value, ObjectView { arena: schema.arena, object: *element }, owner); },
        (Kind::Struct(_), JavaValue::Struct { schema: own_schema, values }) => {
            let own_view = ObjectView { arena: owner, object: *own_schema };
            if !ordinary(own_view, destination, context, depth + 1)? { return Ok(bad_class()); }
            let own = own_view.body()?; let Kind::Struct(fields) = &own.body.kind else { return Ok(bad_class()); };
            if fields.len() != values.len() { return Err(invalid("Typed Struct value has an invalid field-slot count")); }
            for (field, value) in fields.iter().zip(values) {
                let field_view = ObjectView { arena: own.arena, object: field.schema }; let field_schema = field_view.body()?;
                if matches!(value, JavaValue::Null) && (field_schema.optional || field_schema.body.metadata.default.is_some()) { continue; }
                child!(value, field_view, owner);
            }
        }
        (Kind::Struct(_), _) => return Err(error("avro-default-unchecked", "Logical Struct class cannot be cast to Struct during schema validation")),
        (Kind::Array(_), _) | (Kind::Map(_, _), _) => return Err(error("avro-default-unchecked", "Logical container class cannot be cast during schema validation")),
        _ => (),
    }
    Ok(DefaultAttempt::Value(()))
}
