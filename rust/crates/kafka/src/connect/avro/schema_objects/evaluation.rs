use super::*;
use super::{collections::{JavaKey, KeyIdentity}, hashing};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::connect::avro) enum SchemaType { Bool, I8, I16, I32, I64, F32, F64, String, Bytes, Array, Map, Struct }

/// Observations remain consistent for each retained source object/class; evaluation scratch is admitted through the supplied context.
pub(in crate::connect::avro) trait IdentityWitness {
    fn type_hash(&mut self, kind: SchemaType, context: &mut ConversionContext) -> Result<i32>;
    fn builder_hash(&mut self, builder: ObjectView<'_>, context: &mut ConversionContext) -> Result<i32>;
    fn byte_array_hash(&mut self, storage: &Arc<ByteStorage>, context: &mut ConversionContext) -> Result<i32>;
    fn key_hash(&mut self, key: &JavaKey, context: &mut ConversionContext) -> Result<i32>;
    fn pair_hash(&mut self, identity: KeyIdentity, context: &mut ConversionContext) -> Result<i32>;
}

pub(in crate::connect::avro) struct MissingIdentityWitness;
fn missing_identity() -> rtdb_types::Anomaly { rtdb_types::Anomaly::unsupported("rtdb/avro-java-identity-required", "A source Java identity hash observation is required") }
impl IdentityWitness for MissingIdentityWitness {
    fn type_hash(&mut self, _: SchemaType, _: &mut ConversionContext) -> Result<i32> { Err(missing_identity()) }
    fn builder_hash(&mut self, _: ObjectView<'_>, _: &mut ConversionContext) -> Result<i32> { Err(missing_identity()) }
    fn byte_array_hash(&mut self, _: &Arc<ByteStorage>, _: &mut ConversionContext) -> Result<i32> { Err(missing_identity()) }
    fn key_hash(&mut self, _: &JavaKey, _: &mut ConversionContext) -> Result<i32> { Err(missing_identity()) }
    fn pair_hash(&mut self, _: KeyIdentity, _: &mut ConversionContext) -> Result<i32> { Err(missing_identity()) }
}
pub(in crate::connect::avro) struct Evaluation<'a, W> { pub(in crate::connect::avro) context: &'a mut ConversionContext, pub(in crate::connect::avro) witness: &'a mut W }
impl<W: IdentityWitness> rtdb_types::java_hash_map::Admission for Evaluation<'_, W> {
    type Lease = Arc<MemoryReservation>;
    fn reserve(&mut self, bytes: usize) -> Result<Self::Lease> { self.context.reserve(bytes) }
}
fn guard(depth: usize) -> Result<()> { if depth > 128 { Err(invalid("Schema-object comparison depth exceeded")) } else { Ok(()) } }
fn significant(mut bytes: &[u8]) -> &[u8] {
    while bytes.len() > 1 && ((bytes[0] == 0 && bytes[1] & 128 == 0) || (bytes[0] == 255 && bytes[1] & 128 != 0)) { bytes = &bytes[1..]; }
    bytes
}
impl<W: IdentityWitness> Evaluation<'_, W> {
    pub(in crate::connect::avro) fn schema_hash(&mut self, source: ObjectView<'_>) -> Result<i32> { hashing::schema(source, self, 0) }
    pub(in crate::connect::avro) fn value_hash(&mut self, source: &JavaValue, owner: ValueOwner<'_>) -> Result<i32> { hashing::value(source, owner.arena, self, 0) }
    pub(in crate::connect::avro) fn values_equal(&mut self, left: &JavaValue, left_owner: ValueOwner<'_>, right: &JavaValue, right_owner: ValueOwner<'_>, deep: bool) -> Result<bool> { self.values(left, left_owner.arena, right, right_owner.arena, 0, deep) }
    pub(in crate::connect::avro) fn validate_value(&mut self, value: &JavaValue, owner: ValueOwner<'_>, destination: ObjectView<'_>) -> Result<super::super::default_input::DefaultAttempt<()>> { self.validate(value, owner.arena, destination, 0) }
    pub(in crate::connect::avro) fn ordinary(&mut self, left: ObjectView<'_>, right: ObjectView<'_>, depth: usize) -> Result<bool> {
        let left = left.actual()?; let right = right.actual()?;
        if left.identity()? == right.identity()? { return Ok(true); } guard(depth)?;
        match (&left.arena.objects[left.object.index], &right.arena.objects[right.object.index]) {
            (Object::Builder { .. }, Object::Builder { .. }) => Ok(false),
            (Object::Built { .. }, Object::Built { .. }) => self.bodies(left.body()?, right.body()?, depth),
            (Object::Wrapper { target: left_target, optional: left_optional }, Object::Wrapper { target: right_target, optional: right_optional }) => {
                Ok(left_optional == right_optional && self.ordinary(left.child(*left_target), right.child(*right_target), depth + 1)?)
            }
            _ => Ok(false),
        }
    }
    pub(super) fn defaults(&mut self, left: BodyView<'_>, right: BodyView<'_>, depth: usize) -> Result<bool> {
        match (&left.body.metadata.default, &right.body.metadata.default) {
            (None, None) => Ok(true), (Some(left_value), Some(right_value)) => self.values(left_value, left.arena, right_value, right.arena, depth, true), _ => Ok(false),
        }
    }
    pub(in crate::connect::avro) fn properties<'a>(&mut self, left: ObjectView<'a>, right: ObjectView<'a>) -> Result<bool> { super::property_cache::properties(left, right, self) }
    fn bodies(&mut self, left: BodyView<'_>, right: BodyView<'_>, depth: usize) -> Result<bool> {
        let left_meta = &left.body.metadata; let right_meta = &right.body.metadata;
        if left.optional != right.optional || left_meta.version != right_meta.version || left_meta.name != right_meta.name || left_meta.doc != right_meta.doc || hashing::schema_type(&left.body.kind) != hashing::schema_type(&right.body.kind) { return Ok(false); }
        if !self.defaults(left, right, depth + 1)? { return Ok(false); }
        let children_equal = match (&left.body.kind, &right.body.kind) {
            (Kind::Struct(left_fields), Kind::Struct(right_fields)) => {
                for (left_field, right_field) in left_fields.iter().zip(right_fields) {
                    if left_field.name != right_field.name || !self.ordinary(ObjectView { arena: left.arena, object: left_field.schema }, ObjectView { arena: right.arena, object: right_field.schema }, depth + 1)? { return Ok(false); }
                }
                left_fields.len() == right_fields.len()
            }
            (Kind::Array(left_child), Kind::Array(right_child)) => self.ordinary(ObjectView { arena: left.arena, object: *left_child }, ObjectView { arena: right.arena, object: *right_child }, depth + 1)?,
            (Kind::Map(left_key, left_value), Kind::Map(right_key, right_value)) => {
                self.ordinary(ObjectView { arena: left.arena, object: *left_key }, ObjectView { arena: right.arena, object: *right_key }, depth + 1)? && self.ordinary(ObjectView { arena: left.arena, object: *left_value }, ObjectView { arena: right.arena, object: *right_value }, depth + 1)?
            }
            (Kind::Scalar(_), Kind::Scalar(_)) => true, _ => false,
        };
        Ok(children_equal && left_meta.parameters == right_meta.parameters)
    }
    pub(super) fn values(&mut self, left: &JavaValue, left_owner: &SchemaObjects, right: &JavaValue, right_owner: &SchemaObjects, depth: usize, deep: bool) -> Result<bool> {
        if std::ptr::eq(left, right) { return Ok(true); } guard(depth)?;
        use JavaValue as V;
        Ok(match (left, right) {
            (V::Null, V::Null) => true, (V::Bool(left), V::Bool(right)) => left == right,
            (V::I8(left), V::I8(right)) => left == right, (V::I16(left), V::I16(right)) => left == right,
            (V::I32(left), V::I32(right)) => left == right, (V::I64(left), V::I64(right)) => left == right,
            (V::F32(left), V::F32(right)) => left.to_bits() == right.to_bits() || left.is_nan() && right.is_nan(),
            (V::F64(left), V::F64(right)) => left.to_bits() == right.to_bits() || left.is_nan() && right.is_nan(),
            (V::String(left), V::String(right)) => left == right,
            (V::ByteArray(left), V::ByteArray(right)) => if deep { left.bytes() == right.bytes() } else { Arc::ptr_eq(left, right) },
            (V::ByteBuffer(left), V::ByteBuffer(right)) => left.remaining() == right.remaining(),
            (V::Date(left), V::Date(right)) => left == right,
            (V::Decimal(left), V::Decimal(right)) => left.scale == right.scale && significant(&left.coefficient) == significant(&right.coefficient),
            (V::Array(left), V::Array(right)) => self.list(left, left_owner, right, right_owner, depth + 1, false)?,
            (V::Struct { schema: left_schema, values: left }, V::Struct { schema: right_schema, values: right }) => {
                self.ordinary(left_owner.value_schema(*left_schema)?, right_owner.value_schema(*right_schema)?, depth + 1)? && self.list(left, left_owner, right, right_owner, depth + 1, true)?
            }
            (V::Map(left), V::Map(right)) => {
                if left.len() != right.len() { return Ok(false); }
                for (key, value) in left.entries() {
                    let found = right.get(key, left_owner, right_owner, self, depth + 1)?;
                    if matches!(value, V::Null) {
                        if !matches!(found, None | Some(V::Null)) || !right.contains_key(key, left_owner, right_owner, self, depth + 1)? { return Ok(false); }
                    } else {
                        let Some(found) = found else { return Ok(false); };
                        if !self.values(value, left_owner, found, right_owner, depth + 1, false)? { return Ok(false); }
                    }
                }
                true
            }
            _ => false,
        })
    }
    fn list(&mut self, left: &[JavaValue], left_owner: &SchemaObjects, right: &[JavaValue], right_owner: &SchemaObjects, depth: usize, deep: bool) -> Result<bool> {
        if left.len() != right.len() { return Ok(false); }
        for (left, right) in left.iter().zip(right) { if !self.values(left, left_owner, right, right_owner, depth, deep)? { return Ok(false); } }
        Ok(true)
    }
    pub(super) fn validate(&mut self, value: &JavaValue, owner: &SchemaObjects, destination: ObjectView<'_>, depth: usize) -> Result<super::super::default_input::DefaultAttempt<()>> {
        use super::super::default_input::DefaultAttempt;
        let bad_class = || DefaultAttempt::Invalid(error("avro-schema", "Typed default does not match its Java schema object"));
        guard(depth)?; let schema = destination.body()?;
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
        macro_rules! child { ($value:expr, $schema:expr, $owner:expr) => { if let DefaultAttempt::Invalid(cause) = self.validate($value, $owner, $schema, depth + 1)? { return Ok(DefaultAttempt::Invalid(cause)); } }; }
        match (&schema.body.kind, value) {
            (Kind::Array(element), JavaValue::Array(values)) => for value in values { child!(value, ObjectView { arena: schema.arena, object: *element }, owner); },
            (Kind::Map(key, element), JavaValue::Map(entries)) => for (entry_key, value) in entries.iter() {
                child!(entry_key, ObjectView { arena: schema.arena, object: *key }, owner); child!(value, ObjectView { arena: schema.arena, object: *element }, owner);
            },
            (Kind::Struct(_), JavaValue::Struct { schema: own_schema, values }) => {
                let own_view = owner.value_schema(*own_schema)?;
                if !self.ordinary(own_view, destination, depth + 1)? { return Ok(bad_class()); }
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
}

#[cfg(test)]
mod tests;
