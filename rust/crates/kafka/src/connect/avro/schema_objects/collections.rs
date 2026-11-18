use super::*;
use super::{evaluation::{Evaluation, IdentityWitness}, hashing};
use rtdb_types::java_hash_map::{KeyOps, Map, NormalizedMap};

static NEXT_KEY_ID: AtomicU64 = AtomicU64::new(1);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::connect::avro) struct KeyIdentity(RegionId);
#[derive(Clone)]
pub(in crate::connect::avro) struct JavaKey { object: Arc<KeyObject>, _payload: Arc<MemoryReservation>, _allocation: Arc<MemoryReservation> }
struct KeyObject { identity: KeyIdentity, region: RegionId, value: JavaValue }
pub(in crate::connect::avro) struct JavaMapBuilder { map: Map<JavaKey, JavaValue, Arc<MemoryReservation>>, region: RegionId, payload: Arc<MemoryReservation> }
pub(in crate::connect::avro) struct JavaMap { map: NormalizedMap<JavaKey, JavaValue, Arc<MemoryReservation>>, region: RegionId, _payload: Arc<MemoryReservation> }
pub(in crate::connect::avro) struct PreviousValue { value: JavaValue, _payload: Arc<MemoryReservation> }
impl PreviousValue {
    pub(in crate::connect::avro) fn value(&self) -> &JavaValue { &self.value }
    pub(in crate::connect::avro) fn retain(&self, context: &mut ConversionContext) -> Result<()> {
        context.retain(Arc::clone(&self._payload))?; self.value.retain(context, 0)
    }
}

impl JavaKey {
    /// `payload` admits the existing value tree; schema-dependent uses retain its source arena through the supplied owner/import root.
    pub(in crate::connect::avro) fn new(value: JavaValue, owner: ValueOwner<'_>, payload: Arc<MemoryReservation>, context: &mut ConversionContext) -> Result<Self> {
        let identity = KeyIdentity::new()?; let allocation = context.reserve(add(size_of::<KeyObject>(), 4096)?)?;
        Ok(Self { object: Arc::new(KeyObject { identity, region: owner.arena.region, value }), _payload: payload, _allocation: allocation })
    }
    pub(in crate::connect::avro) fn identity(&self) -> KeyIdentity { self.object.identity }
    pub(in crate::connect::avro) fn value(&self) -> &JavaValue { &self.object.value }
    pub(super) fn owner<'a>(&self, root: &'a SchemaObjects) -> Result<&'a SchemaObjects> { root.find_region(self.object.region, 0) }
    fn same_object(&self, other: &Self) -> bool {
        match (self.value(), other.value()) { (JavaValue::ByteArray(left), JavaValue::ByteArray(right)) => Arc::ptr_eq(left, right), _ => Arc::ptr_eq(&self.object, &other.object) }
    }
    pub(super) fn class_name(&self) -> &str {
        match self.value() {
            JavaValue::Null => "", JavaValue::Bool(_) => "java.lang.Boolean", JavaValue::I8(_) => "java.lang.Byte",
            JavaValue::I16(_) => "java.lang.Short", JavaValue::I32(_) => "java.lang.Integer", JavaValue::I64(_) => "java.lang.Long",
            JavaValue::F32(_) => "java.lang.Float", JavaValue::F64(_) => "java.lang.Double", JavaValue::String(_) | JavaValue::StringUnits(_) => "java.lang.String",
            JavaValue::ByteArray(_) => "[B", JavaValue::ByteBuffer(buffer) => buffer.class_name(), JavaValue::Date(_) => "java.util.Date",
            JavaValue::Decimal(_) => "java.math.BigDecimal", JavaValue::Array(_) => "java.util.ArrayList", JavaValue::Map(_) => "java.util.HashMap",
            JavaValue::Struct { .. } => "org.apache.kafka.connect.data.Struct",
        }
    }
}
impl KeyIdentity {
    pub(in crate::connect::avro) fn new() -> Result<Self> {
        NEXT_KEY_ID.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| next.checked_add(1)).map(|identity| Self(RegionId(identity))).map_err(|_| error("allocation-overflow", "Typed key identity space is exhausted"))
    }
}
struct Keys<'a, 'b> { candidate_owner: &'a SchemaObjects, stored_owner: &'b SchemaObjects, depth: usize }
impl<'e, W: IdentityWitness> KeyOps<JavaKey, Evaluation<'e, W>> for Keys<'_, '_> {
    type Comparable = hashing::Comparable;
    fn is_null(&self, key: &JavaKey) -> bool { matches!(key.value(), JavaValue::Null) }
    fn same_identity(&self, key: &JavaKey, stored: &JavaKey) -> bool { key.same_object(stored) }
    fn hash_code(&mut self, key: &JavaKey, evaluation: &mut Evaluation<'e, W>) -> Result<i32> { hashing::value(key.value(), self.candidate_owner.find_region(key.object.region, 0)?, evaluation, self.depth) }
    fn equals(&mut self, key: &JavaKey, stored: &JavaKey, evaluation: &mut Evaluation<'e, W>) -> Result<bool> {
        evaluation.values(key.value(), self.candidate_owner.find_region(key.object.region, 0)?, stored.value(), self.stored_owner.find_region(stored.object.region, 0)?, self.depth, false)
    }
    fn comparable_class(&mut self, key: &JavaKey, _: &mut Evaluation<'e, W>) -> Result<Option<Self::Comparable>> { Ok(hashing::comparable(key.value())) }
    fn compare(&mut self, class: Self::Comparable, key: &JavaKey, stored: &JavaKey, evaluation: &mut Evaluation<'e, W>) -> Result<i32> { hashing::compare(class, key.value(), stored.value(), evaluation.context) }
    fn class_name<'a>(&'a self, key: &'a JavaKey) -> &'a str { key.class_name() }
    fn identity_hash_code(&mut self, key: &JavaKey, evaluation: &mut Evaluation<'e, W>) -> Result<i32> {
        match key.value() {
            JavaValue::ByteArray(storage) => evaluation.witness.byte_array_hash(storage, evaluation.context),
            _ => evaluation.witness.key_hash(key, evaluation.context),
        }
    }
}
impl JavaMapBuilder {
    pub(in crate::connect::avro) fn new(initial: Option<usize>, owner: ValueOwner<'_>, payload: Arc<MemoryReservation>) -> Result<Self> { Ok(Self { map: Map::new(initial)?, region: owner.arena.region, payload }) }
    pub(in crate::connect::avro) fn put(&mut self, key: JavaKey, value: JavaValue, owner: ValueOwner<'_>, evaluation: &mut Evaluation<'_, impl IdentityWitness>) -> Result<Option<PreviousValue>> {
        if owner.arena.region != self.region { return Err(invalid("Typed Map construction changed its owning region")); }
        Ok(self.map.put(key, value, &mut Keys { candidate_owner: owner.arena, stored_owner: owner.arena, depth: 0 }, evaluation)?.map(|value| PreviousValue { value, _payload: Arc::clone(&self.payload) }))
    }
    pub(in crate::connect::avro) fn finish(self, evaluation: &mut Evaluation<'_, impl IdentityWitness>) -> Result<JavaMap> { Ok(JavaMap { map: self.map.finish(evaluation)?, region: self.region, _payload: self.payload }) }
}
impl JavaMap {
    pub(super) fn owner<'a>(&self, root: &'a SchemaObjects) -> Result<&'a SchemaObjects> { root.find_region(self.region, 0) }
    pub(in crate::connect::avro) fn len(&self) -> usize { self.map.len() }
    pub(in crate::connect::avro) fn iter(&self) -> impl ExactSizeIterator<Item = (&JavaValue, &JavaValue)> { self.map.iter().map(|(key, value)| (key.value(), value)) }
    pub(in crate::connect::avro) fn entries(&self) -> impl ExactSizeIterator<Item = (&JavaKey, &JavaValue)> { self.map.iter() }
    pub(super) fn get(&self, key: &JavaKey, candidate_owner: &SchemaObjects, stored_owner: &SchemaObjects, evaluation: &mut Evaluation<'_, impl IdentityWitness>, depth: usize) -> Result<Option<&JavaValue>> {
        let stored_owner = stored_owner.find_region(self.region, 0)?;
        self.map.get(key, &mut Keys { candidate_owner, stored_owner, depth }, evaluation)
    }
    pub(super) fn contains_key(&self, key: &JavaKey, candidate_owner: &SchemaObjects, stored_owner: &SchemaObjects, evaluation: &mut Evaluation<'_, impl IdentityWitness>, depth: usize) -> Result<bool> {
        let stored_owner = stored_owner.find_region(self.region, 0)?;
        self.map.contains_key(key, &mut Keys { candidate_owner, stored_owner, depth }, evaluation)
    }
    pub(in crate::connect::avro) fn retain(&self, context: &mut ConversionContext) -> Result<()> {
        context.retain(Arc::clone(&self._payload))?;
        for charge in self.map.leases() { context.retain(Arc::clone(charge))?; }
        for (key, _) in self.map.iter() { context.retain(Arc::clone(&key._payload))?; context.retain(Arc::clone(&key._allocation))?; }
        Ok(())
    }
    /// The caller retains the creating schema family when entries contain Struct IDs.
    pub(in crate::connect::avro) fn retain_contents(&self, context: &mut ConversionContext) -> Result<()> {
        self.retain_at(context, 0)
    }
    pub(super) fn retain_at(&self, context: &mut ConversionContext, depth: usize) -> Result<()> {
        if depth > 128 { return Err(invalid("Typed value nesting depth exceeded")); }
        self.retain(context)?;
        for (key, value) in self.map.iter() { key.value().retain(context, depth + 1)?; value.retain(context, depth + 1)?; }
        Ok(())
    }
}
impl std::fmt::Debug for JavaMap {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.debug_map().entries(self.iter()).finish() }
}
