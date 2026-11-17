use super::*;
use rtdb_types::java_hash_map::{KeyOps, Map, PutOutcome};

#[derive(Clone, Copy)]
struct Pair<'a> { left: ObjectView<'a>, right: ObjectView<'a>, identity: KeyIdentity }
struct PairOps { depth: usize }
impl<'s, 'e, W: IdentityWitness> KeyOps<Pair<'s>, Evaluation<'e, W>> for PairOps {
    type Comparable = ();
    fn is_null(&self, _: &Pair<'s>) -> bool { false }
    fn same_identity(&self, key: &Pair<'s>, stored: &Pair<'s>) -> bool { key.identity == stored.identity }
    fn hash_code(&mut self, key: &Pair<'s>, evaluation: &mut Evaluation<'e, W>) -> Result<i32> {
        let left = hashing::schema(key.left, evaluation, self.depth + 1)?;
        let right = hashing::schema(key.right, evaluation, self.depth + 1)?;
        Ok(31i32.wrapping_add(left).wrapping_mul(31).wrapping_add(right))
    }
    fn equals(&mut self, key: &Pair<'s>, stored: &Pair<'s>, evaluation: &mut Evaluation<'e, W>) -> Result<bool> {
        Ok(evaluation.ordinary(key.left, stored.left, self.depth + 1)? && evaluation.ordinary(key.right, stored.right, self.depth + 1)?)
    }
    fn comparable_class(&mut self, _: &Pair<'s>, _: &mut Evaluation<'e, W>) -> Result<Option<()>> { Ok(None) }
    fn compare(&mut self, _: (), _: &Pair<'s>, _: &Pair<'s>, _: &mut Evaluation<'e, W>) -> Result<i32> { Ok(0) }
    fn class_name<'a>(&'a self, _: &'a Pair<'s>) -> &'a str { "io.confluent.connect.avro.AvroData$Pair" }
    fn identity_hash_code(&mut self, key: &Pair<'s>, evaluation: &mut Evaluation<'e, W>) -> Result<i32> { evaluation.witness.pair_hash(key.identity, evaluation.context) }
}

type Cache<'a> = Map<Pair<'a>, bool, Arc<MemoryReservation>>;
pub(super) fn properties<'a>(left: ObjectView<'a>, right: ObjectView<'a>, evaluation: &mut Evaluation<'_, impl IdentityWitness>) -> Result<bool> {
    structural(left, right, &mut Map::new(None)?, evaluation, 0)
}
fn structural<'a>(left: ObjectView<'a>, right: ObjectView<'a>, cache: &mut Cache<'a>, evaluation: &mut Evaluation<'_, impl IdentityWitness>, depth: usize) -> Result<bool> {
    if left.identity()? == right.identity()? { return Ok(true); }
    let pair = Pair { left, right, identity: KeyIdentity::new()? };
    match cache.put_if_absent(pair, true, |_| false, &mut PairOps { depth }, evaluation)? {
        PutOutcome::Present(existing) => return Ok(*existing), PutOutcome::Inserted => (), PutOutcome::Replaced(_) => unreachable!("pair-cache Boolean values are non-null"),
    }
    if depth > 128 { return Err(invalid("Schema-object comparison depth exceeded")); }
    let left = left.body()?; let right = right.body()?;
    let left_meta = &left.body.metadata; let right_meta = &right.body.metadata;
    let mut equal = left.optional == right.optional && left_meta.version == right_meta.version && left_meta.name == right_meta.name && left_meta.doc == right_meta.doc
        && hashing::schema_type(&left.body.kind) == hashing::schema_type(&right.body.kind)
        && evaluation.defaults(left, right, depth + 1)? && left_meta.parameters == right_meta.parameters;
    if equal {
        equal = match (&left.body.kind, &right.body.kind) {
            (Kind::Struct(left_fields), Kind::Struct(right_fields)) => {
                let mut fields_equal = true;
                for (left_field, right_field) in left_fields.iter().zip(right_fields) {
                    if left_field.name != right_field.name || !structural(ObjectView { arena: left.arena, object: left_field.schema }, ObjectView { arena: right.arena, object: right_field.schema }, cache, evaluation, depth + 1)? { fields_equal = false; break; }
                }
                fields_equal && left_fields.len() == right_fields.len()
            }
            (Kind::Array(left_child), Kind::Array(right_child)) => structural(ObjectView { arena: left.arena, object: *left_child }, ObjectView { arena: right.arena, object: *right_child }, cache, evaluation, depth + 1)?,
            (Kind::Map(left_key, left_value), Kind::Map(right_key, right_value)) => {
                structural(ObjectView { arena: left.arena, object: *left_value }, ObjectView { arena: right.arena, object: *right_value }, cache, evaluation, depth + 1)?
                    && structural(ObjectView { arena: left.arena, object: *left_key }, ObjectView { arena: right.arena, object: *right_key }, cache, evaluation, depth + 1)?
            }
            (Kind::Scalar(_), Kind::Scalar(_)) => true, _ => false,
        };
    }
    cache.put(pair, equal, &mut PairOps { depth }, evaluation)?;
    Ok(equal)
}

#[cfg(test)]
mod tests;
