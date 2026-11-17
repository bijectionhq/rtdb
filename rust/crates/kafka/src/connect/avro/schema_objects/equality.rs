use super::*;
use super::super::default_input::DefaultAttempt;

#[derive(Clone, Copy)]
struct Pair { left: (RegionId, usize), right: (RegionId, usize), equal: bool }
#[derive(Clone, Copy)]
enum ValueRelation { Ordinary, Deep }

fn depth_guard(depth: usize) -> Result<()> { if depth > 128 { Err(invalid("Schema-object comparison depth exceeded")) } else { Ok(()) } }

pub(super) fn ordinary(left: ObjectView<'_>, right: ObjectView<'_>, context: &mut ConversionContext, depth: usize) -> Result<bool> {
    Evaluation { context, witness: &mut MissingIdentityWitness }.ordinary(left, right, depth)
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
fn values(left: &JavaValue, left_arena: &SchemaObjects, right: &JavaValue, right_arena: &SchemaObjects, context: &mut ConversionContext, depth: usize, relation: ValueRelation) -> Result<bool> {
    Evaluation { context, witness: &mut MissingIdentityWitness }.values(left, left_arena, right, right_arena, depth, matches!(relation, ValueRelation::Deep))
}

pub(super) fn validate(value: &JavaValue, owner: &SchemaObjects, destination: ObjectView<'_>, context: &mut ConversionContext, depth: usize) -> Result<DefaultAttempt<()>> {
    Evaluation { context, witness: &mut MissingIdentityWitness }.validate(value, owner, destination, depth)
}
