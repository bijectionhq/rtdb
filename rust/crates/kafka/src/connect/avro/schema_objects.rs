use std::{collections::BTreeMap, mem::size_of, sync::{Arc, OnceLock, atomic::{AtomicU64, Ordering}}};
use rtdb_columnar::MemoryReservation;
use rtdb_types::Result;
use super::{error, super::{ConversionContext, schema_graph::Scalar}};

mod equality;
mod binary;
mod hashing;
mod collections;
mod evaluation;
mod property_cache;
pub(super) use binary::{ByteStorage, ByteBuffer, BufferBacking, BufferOrder};
pub(super) use collections::{JavaKey, JavaMapBuilder, JavaMap, KeyIdentity, PreviousValue};
pub(super) use evaluation::{Evaluation, IdentityWitness, MissingIdentityWitness, SchemaType};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ObjectId { region: RegionId, index: usize }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RegionId(u64);
static NEXT_REGION_ID: AtomicU64 = AtomicU64::new(1);
fn take_region_id(counter: &AtomicU64) -> Result<RegionId> {
    counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| next.checked_add(1)).map(RegionId).map_err(|_| error("allocation-overflow", "Schema-object region identity space is exhausted"))
}
#[derive(Clone)]
pub(super) struct SchemaHandle { arena: Arc<SchemaObjects>, object: ObjectId }
#[derive(Clone, Copy)]
pub(super) struct ObjectView<'a> { arena: &'a SchemaObjects, object: ObjectId }
#[derive(Clone, Copy)]
pub(super) struct ValueOwner<'a> { arena: &'a SchemaObjects }

pub(super) struct SchemaObjectsBuilder { arena: SchemaObjects }
struct SchemaObjects { region: RegionId, objects: Vec<Object>, charges: Vec<Arc<MemoryReservation>> }
enum Object { Builder { body: Body, state: BuilderState }, Built { builder: ObjectId, hash: OnceLock<i32> }, Wrapper { target: ObjectId, optional: bool }, Imported(SchemaHandle) }
enum BuilderState { Building, Complete }
struct Body { kind: Kind, metadata: Metadata }
enum Kind { Scalar(Scalar), Array(ObjectId), Map(ObjectId, ObjectId), Struct(Vec<Field>) }
struct Field { name: String, schema: ObjectId }
#[derive(Default)]
struct Metadata {
    optional: bool, name: Option<String>, version: Option<i32>, doc: Option<String>,
    parameters: Option<BTreeMap<String, String>>, default: Option<JavaValue>,
}

#[derive(Debug)]
pub(super) enum JavaValue {
    Null, Bool(bool), I8(i8), I16(i16), I32(i32), I64(i64), F32(f32), F64(f64),
    String(String), ByteArray(Arc<ByteStorage>), ByteBuffer(ByteBuffer), Date(i64), Decimal(BigDecimal),
    Array(Vec<JavaValue>), Map(JavaMap), Struct { schema: ObjectId, values: Vec<JavaValue> },
}
#[derive(Debug)]
pub(super) struct BigDecimal { coefficient: Vec<u8>, scale: i32 }
impl BigDecimal {
    pub(super) fn new(coefficient: Vec<u8>, scale: i32) -> Result<Self> {
        if coefficient.is_empty() { return Err(error("avro-default-unchecked", "BigDecimal coefficient bytes are empty")); }
        Ok(Self { coefficient, scale })
    }
    pub(super) fn coefficient(&self) -> &[u8] { &self.coefficient }
    pub(super) fn scale(&self) -> i32 { self.scale }
}
#[derive(Clone, Copy)]
struct BodyView<'a> { arena: &'a SchemaObjects, body: &'a Body, optional: bool }

fn invalid(message: &'static str) -> rtdb_types::Anomaly { error("avro-schema-objects", message) }
fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(super::resolution::overflow) }
fn mul(left: usize, right: usize) -> Result<usize> { left.checked_mul(right).ok_or_else(super::resolution::overflow) }
fn capacity<T>(values: &Vec<T>) -> Result<Option<(usize, usize)>> {
    if values.len() < values.capacity() { return Ok(None); }
    let next = if values.capacity() == 0 { 4 } else { mul(values.capacity(), 2)? };
    Ok(Some((next, add(mul(mul(next, size_of::<T>())?, 16)?, 256)?)))
}
fn grow<T>(values: &mut Vec<T>, next: Option<(usize, usize)>) -> Result<()> {
    if let Some((next, _)) = next { values.try_reserve_exact(next - values.len()).map_err(|cause| error("allocation-failed", cause))?; }
    Ok(())
}

impl SchemaObjectsBuilder {
    pub(super) fn new(context: &mut ConversionContext) -> Result<Self> {
        let region = take_region_id(&NEXT_REGION_ID)?;
        let charge = context.reserve(4096)?;
        Ok(Self { arena: SchemaObjects { region, objects: Vec::new(), charges: vec![charge] } })
    }
    fn admit(&mut self, bytes: usize, context: &mut ConversionContext) -> Result<Arc<MemoryReservation>> {
        let next = capacity(&self.arena.charges)?;
        let charge = context.reserve(add(bytes, next.map_or(0, |(_, bytes)| bytes))?)?;
        grow(&mut self.arena.charges, next)?;
        self.arena.charges.push(Arc::clone(&charge));
        Ok(charge)
    }
    fn insert(&mut self, object: Object, context: &mut ConversionContext) -> Result<ObjectId> {
        let next = capacity(&self.arena.objects)?;
        self.admit(next.map_or(0, |(_, bytes)| bytes), context)?;
        grow(&mut self.arena.objects, next)?;
        let id = ObjectId { region: self.arena.region_id(), index: self.arena.objects.len() }; self.arena.objects.push(object); Ok(id)
    }
    fn building(&mut self, id: ObjectId) -> Result<&mut Body> {
        self.require(id)?;
        match self.arena.objects.get_mut(id.index) {
            Some(Object::Builder { body, state: BuilderState::Building }) => Ok(body),
            _ => Err(invalid("Schema mutation requires an unfinished local builder")),
        }
    }
    fn require(&self, id: ObjectId) -> Result<()> { self.arena.require(id) }
    pub(super) fn view(&self, id: ObjectId) -> Result<ObjectView<'_>> { self.require(id)?; Ok(ObjectView { arena: &self.arena, object: id }) }
    pub(super) fn owner(&self) -> ValueOwner<'_> { ValueOwner { arena: &self.arena } }
    fn builder(&mut self, kind: Kind, context: &mut ConversionContext) -> Result<ObjectId> { self.insert(Object::Builder { body: Body { kind, metadata: Metadata::default() }, state: BuilderState::Building }, context) }
    pub(super) fn scalar(&mut self, kind: Scalar, context: &mut ConversionContext) -> Result<ObjectId> { self.builder(Kind::Scalar(kind), context) }
    pub(super) fn record(&mut self, context: &mut ConversionContext) -> Result<ObjectId> { self.builder(Kind::Struct(Vec::new()), context) }
    pub(super) fn array(&mut self, element: ObjectId, context: &mut ConversionContext) -> Result<ObjectId> { self.require(element)?; self.builder(Kind::Array(element), context) }
    pub(super) fn map(&mut self, key: ObjectId, value: ObjectId, context: &mut ConversionContext) -> Result<ObjectId> { self.require(key)?; self.require(value)?; self.builder(Kind::Map(key, value), context) }
    pub(super) fn field(&mut self, builder: ObjectId, name: &str, schema: ObjectId, context: &mut ConversionContext) -> Result<()> {
        self.require(schema)?;
        let Kind::Struct(fields) = &self.building(builder)?.kind else { return Err(invalid("Only a Struct builder can receive fields")); };
        if fields.iter().any(|field| field.name == name) { return Err(invalid("Schema builder has duplicate Struct fields")); }
        let next = capacity(fields)?;
        self.admit(add(name.len(), next.map_or(0, |(_, bytes)| bytes))?, context)?;
        let Kind::Struct(fields) = &mut self.building(builder)?.kind else { unreachable!() };
        grow(fields, next)?; fields.push(Field { name: name.into(), schema }); Ok(())
    }
    pub(super) fn optional(&mut self, builder: ObjectId, optional: bool) -> Result<()> { self.building(builder)?.metadata.optional = optional; Ok(()) }
    pub(super) fn version(&mut self, builder: ObjectId, version: Option<i32>) -> Result<()> { self.building(builder)?.metadata.version = version; Ok(()) }
    pub(super) fn name(&mut self, builder: ObjectId, name: Option<&str>, context: &mut ConversionContext) -> Result<()> {
        self.building(builder)?;
        self.admit(add(256, name.map_or(0, str::len))?, context)?;
        self.building(builder)?.metadata.name = name.map(str::to_owned); Ok(())
    }
    pub(super) fn doc(&mut self, builder: ObjectId, doc: Option<&str>, context: &mut ConversionContext) -> Result<()> {
        self.building(builder)?;
        self.admit(add(256, doc.map_or(0, str::len))?, context)?;
        self.building(builder)?.metadata.doc = doc.map(str::to_owned); Ok(())
    }
    pub(super) fn parameter(&mut self, builder: ObjectId, name: &str, value: &str, context: &mut ConversionContext) -> Result<()> {
        self.building(builder)?;
        self.admit(add(add(name.len(), value.len())?, 1024)?, context)?;
        self.building(builder)?.metadata.parameters.get_or_insert_with(BTreeMap::new).insert(name.into(), value.into()); Ok(())
    }
    pub(super) fn byte_array(&mut self, bytes: &[u8], context: &mut ConversionContext) -> Result<JavaValue> {
        if bytes.len() > i32::MAX as usize { return Err(error("batch-too-large", "Java byte-array length exceeds its signed 32-bit domain")); }
        let charge = self.admit(add(bytes.len(), 4096)?, context)?;
        Ok(JavaValue::ByteArray(Arc::new(ByteStorage::new(bytes.to_vec(), charge))))
    }
    pub(super) fn byte_buffer(&mut self, bytes: &[u8], context: &mut ConversionContext) -> Result<JavaValue> {
        let JavaValue::ByteArray(storage) = self.byte_array(bytes, context)? else { unreachable!() };
        Ok(JavaValue::ByteBuffer(ByteBuffer::whole(storage)))
    }
    /// `bound` covers every allocation performed by `materialize`, including its typed container slots and payloads.
    pub(super) fn default_with(&mut self, builder: ObjectId, bound: usize, context: &mut ConversionContext, materialize: impl FnOnce() -> Result<JavaValue>) -> Result<super::default_input::DefaultAttempt<()>> {
        self.default_typed_with(builder, bound, context, &mut MissingIdentityWitness, |_, _, _| materialize())
    }
    /// `bound` covers callback-owned value slots/payloads; returned typed owners retain their separately admitted storage leases.
    pub(super) fn default_typed_with<W: IdentityWitness>(&mut self, builder: ObjectId, bound: usize, context: &mut ConversionContext, witness: &mut W, materialize: impl FnOnce(ValueOwner<'_>, Arc<MemoryReservation>, &mut Evaluation<'_, W>) -> Result<JavaValue>) -> Result<super::default_input::DefaultAttempt<()>> {
        self.building(builder)?; let payload = self.admit(bound, context)?;
        let mut evaluation = Evaluation { context, witness };
        let value = materialize(ValueOwner { arena: &self.arena }, payload, &mut evaluation)?;
        let validation = evaluation.validate(&value, &self.arena, ObjectView { arena: &self.arena, object: builder }, 0)?;
        if let super::default_input::DefaultAttempt::Invalid(cause) = validation { return Ok(super::default_input::DefaultAttempt::Invalid(cause)); }
        self.building(builder)?.metadata.default = if matches!(value, JavaValue::Null) { None } else { Some(value) }; Ok(super::default_input::DefaultAttempt::Value(()))
    }
    pub(super) fn wrapper(&mut self, target: ObjectId, optional: bool, context: &mut ConversionContext) -> Result<ObjectId> { self.require(target)?; self.insert(Object::Wrapper { target, optional }, context) }
    pub(super) fn build(&mut self, builder: ObjectId, context: &mut ConversionContext) -> Result<ObjectId> {
        self.building(builder)?;
        let built = self.insert(Object::Built { builder, hash: OnceLock::new() }, context)?;
        match &mut self.arena.objects[builder.index] { Object::Builder { state, .. } => *state = BuilderState::Complete, _ => unreachable!() };
        Ok(built)
    }
    pub(super) fn import(&mut self, handle: &SchemaHandle, context: &mut ConversionContext) -> Result<ObjectId> {
        for (index, object) in self.arena.objects.iter().enumerate() {
            if let Object::Imported(existing) = object { if Arc::ptr_eq(&existing.arena, &handle.arena) && existing.view().identity()? == handle.view().identity()? { return Ok(ObjectId { region: self.arena.region_id(), index }); } }
        }
        handle.retain(context)?;
        self.insert(Object::Imported(handle.clone()), context)
    }
    pub(super) fn finish(mut self, root: ObjectId, context: &mut ConversionContext) -> Result<SchemaHandle> {
        self.require(root)?;
        if self.arena.objects.iter().any(|object| matches!(object, Object::Builder { state: BuilderState::Building, .. })) { return Err(invalid("An unfinished schema builder cannot be published")); }
        self.admit(4096, context)?;
        Ok(SchemaHandle { arena: Arc::new(self.arena), object: root })
    }
}

impl SchemaObjects {
    fn region_id(&self) -> RegionId { self.region }
    fn require(&self, id: ObjectId) -> Result<()> {
        if id.region == self.region_id() && id.index < self.objects.len() { Ok(()) } else { Err(invalid("Schema object is outside its owning region")) }
    }
    fn find_region(&self, region: RegionId, depth: usize) -> Result<&Self> {
        fn find(arena: &SchemaObjects, region: RegionId, depth: usize) -> Result<Option<&SchemaObjects>> {
            if depth > 128 { return Err(invalid("Imported typed-value owner depth exceeded")); }
            if arena.region == region { return Ok(Some(arena)); }
            for object in &arena.objects { if let Object::Imported(handle) = object { if let Some(owner) = find(&handle.arena, region, depth + 1)? { return Ok(Some(owner)); } } }
            Ok(None)
        }
        find(self, region, depth)?.ok_or_else(|| invalid("Typed value's original schema owner is not retained by this region"))
    }
    fn value_schema(&self, object: ObjectId) -> Result<ObjectView<'_>> { Ok(ObjectView { arena: self.find_region(object.region, 0)?, object }) }
}

impl JavaValue {
    fn retain(&self, context: &mut ConversionContext, depth: usize) -> Result<()> {
        if depth > 128 { return Err(invalid("Typed value nesting depth exceeded")); }
        match self {
            JavaValue::ByteArray(storage) => storage.retain(context)?,
            JavaValue::ByteBuffer(buffer) => buffer.retain(context)?,
            JavaValue::Array(children) | JavaValue::Struct { values: children, .. } => for child in children { child.retain(context, depth + 1)?; },
            JavaValue::Map(entries) => { entries.retain(context)?; for (key, child) in entries.iter() { key.retain(context, depth + 1)?; child.retain(context, depth + 1)?; } },
            _ => (),
        }
        Ok(())
    }
}

impl SchemaHandle {
    pub(super) fn owner(&self) -> ValueOwner<'_> { ValueOwner { arena: &self.arena } }
    pub(super) fn view(&self) -> ObjectView<'_> { ObjectView { arena: &self.arena, object: self.object } }
    pub(super) fn at(&self, object: ObjectId) -> Result<ObjectView<'_>> { self.arena.require(object)?; Ok(ObjectView { arena: &self.arena, object }) }
    pub(super) fn retain(&self, context: &mut ConversionContext) -> Result<()> {
        fn walk(arena: &SchemaObjects, seen: &mut Vec<RegionId>, context: &mut ConversionContext, depth: usize) -> Result<()> {
            if depth > 128 { return Err(invalid("Imported schema-object depth exceeded")); }
            let owner = arena.region;
            if seen.contains(&owner) { return Ok(()); }
            let next = capacity(seen)?;
            if let Some((_, bytes)) = next { context.reserve(bytes)?; }
            grow(seen, next)?; seen.push(owner);
            for charge in &arena.charges { context.retain(Arc::clone(charge))?; }
            for object in &arena.objects {
                match object {
                    Object::Imported(imported) => walk(&imported.arena, seen, context, depth + 1)?,
                    Object::Builder { body, .. } => if let Some(value) = &body.metadata.default { value.retain(context, 0)?; },
                    _ => (),
                }
            }
            Ok(())
        }
        walk(&self.arena, &mut Vec::new(), context, 0)
    }
}
impl<'a> ObjectView<'a> {
    fn actual(mut self) -> Result<Self> {
        for _ in 0..=128 {
            self.arena.require(self.object)?;
            match self.arena.objects.get(self.object.index) {
                Some(Object::Imported(handle)) => self = handle.view(),
                Some(_) => return Ok(self),
                None => return Err(invalid("Schema object is outside its owning region")),
            }
        }
        Err(invalid("Imported schema-object depth exceeded"))
    }
    fn identity(self) -> Result<(RegionId, usize)> { let actual = self.actual()?; Ok((actual.arena.region, actual.object.index)) }
    pub(super) fn object_id(self) -> Result<ObjectId> { Ok(self.actual()?.object) }
    fn child(self, object: ObjectId) -> Self { Self { arena: self.arena, object } }
    fn body(self) -> Result<BodyView<'a>> {
        let mut current = self; let mut optional = None;
        for _ in 0..=128 {
            current = current.actual()?;
            match &current.arena.objects[current.object.index] {
                Object::Wrapper { target, optional: outer } => { optional.get_or_insert(*outer); current = current.child(*target); }
                Object::Built { builder, .. } => current = current.child(*builder),
                Object::Builder { body, .. } => return Ok(BodyView { arena: current.arena, body, optional: optional.unwrap_or(body.metadata.optional) }),
                _ => return Err(invalid("Schema object body is not complete")),
            }
        }
        Err(invalid("Schema-object wrapper depth exceeded"))
    }
    pub(super) fn ordinary_equals(self, other: Self, context: &mut ConversionContext) -> Result<bool> { equality::ordinary(self, other, context, 0) }
    pub(super) fn property_equals(self, other: Self, context: &mut ConversionContext) -> Result<bool> { equality::properties(self, other, context) }
}

#[cfg(test)]
mod tests;
