use super::*;
use rtdb_types::java_hash_map::{Admission, KeyOps, Map};
use super::identity::{IdentityOps, IdentityTable};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::connect::avro) struct PairId(u64);
static NEXT_PAIR: AtomicU64 = AtomicU64::new(1);
pub(in crate::connect::avro) trait Witness {
    fn type_hash(&mut self, kind: Type, context: &mut ConversionContext) -> Result<i32>;
    fn schema_identity(&mut self, schema: &SchemaHandle, context: &mut ConversionContext) -> Result<i32>;
    fn pair_identity(&mut self, pair: PairId, context: &mut ConversionContext) -> Result<i32>;
}
pub(super) struct Authority<'a, W> {
    pub(super) context: &'a mut ConversionContext,
    pub(super) witness: &'a mut W,
}
impl<W: Witness> Admission for Authority<'_, W> {
    type Lease = Arc<MemoryReservation>;
    fn reserve(&mut self, bytes: usize) -> Result<Self::Lease> { self.context.reserve(bytes) }
}
pub(super) struct SchemaIdentityOps;
impl<W: Witness> IdentityOps<SchemaHandle, Authority<'_, W>> for SchemaIdentityOps {
    fn same(&self, key: &SchemaHandle, stored: &SchemaHandle) -> bool { key.same_object(stored) }
    fn hash(&mut self, key: &SchemaHandle, authority: &mut Authority<'_, W>) -> Result<i32> {
        authority.witness.schema_identity(key, authority.context)
    }
}
#[derive(Clone)]
struct SeenPair { left: SchemaHandle, right: SchemaHandle, id: PairId }
struct SeenOps;
impl<W: Witness> KeyOps<SeenPair, Authority<'_, W>> for SeenOps {
    type Comparable = ();
    fn is_null(&self, _: &SeenPair) -> bool { false }
    fn same_identity(&self, key: &SeenPair, stored: &SeenPair) -> bool { key.id == stored.id }
    fn hash_code(&mut self, key: &SeenPair, authority: &mut Authority<'_, W>) -> Result<i32> {
        let left = authority.witness.schema_identity(&key.left, authority.context)?;
        let right = authority.witness.schema_identity(&key.right, authority.context)?;
        Ok(left.wrapping_add(right))
    }
    fn equals(&mut self, key: &SeenPair, stored: &SeenPair, _: &mut Authority<'_, W>) -> Result<bool> {
        Ok(key.left.same_object(&stored.left) && key.right.same_object(&stored.right))
    }
    fn comparable_class(&mut self, _: &SeenPair, _: &mut Authority<'_, W>) -> Result<Option<()>> { Ok(None) }
    fn compare(&mut self, _: (), _: &SeenPair, _: &SeenPair, _: &mut Authority<'_, W>) -> Result<i32> { Ok(0) }
    fn class_name<'a>(&'a self, _: &'a SeenPair) -> &'a str { "org.apache.avro.Schema$SeenPair" }
    fn identity_hash_code(&mut self, key: &SeenPair, authority: &mut Authority<'_, W>) -> Result<i32> {
        authority.witness.pair_identity(key.id, authority.context)
    }
}
enum Lazy<T> { Cold, Ready(T) }
type HashSeen = IdentityTable<SchemaHandle, (), Arc<MemoryReservation>>;
type EqualSeen = Map<SeenPair, (), Arc<MemoryReservation>>;
pub(in crate::connect::avro) struct RecursionState { hash: Lazy<HashSeen>, equal: Lazy<EqualSeen>, failed: bool }
impl RecursionState {
    pub(in crate::connect::avro) fn new() -> Self { Self { hash: Lazy::Cold, equal: Lazy::Cold, failed: false } }
    fn usable(&self) -> Result<()> {
        if self.failed { Err(invalid("Failed Avro recursion state cannot be reused")) } else { Ok(()) }
    }
    fn hash<W: Witness>(&mut self, authority: &mut Authority<'_, W>) -> Result<&mut HashSeen> {
        self.usable()?;
        if matches!(self.hash, Lazy::Cold) { self.hash = Lazy::Ready(HashSeen::new(authority)?); }
        let Lazy::Ready(table) = &mut self.hash else { unreachable!() };
        Ok(table)
    }
    fn equal(&mut self) -> Result<&mut EqualSeen> {
        self.usable()?;
        if matches!(self.equal, Lazy::Cold) { self.equal = Lazy::Ready(Map::new(None)?); }
        let Lazy::Ready(table) = &mut self.equal else { unreachable!() };
        Ok(table)
    }
    fn retain(&self, context: &mut ConversionContext) -> Result<()> {
        self.usable()?;
        if let Lazy::Ready(table) = &self.hash {
            if !table.is_empty() { return Err(invalid("Avro hash recursion is already active")); }
            for charge in table.leases() { context.retain(Arc::clone(charge))?; }
        }
        if let Lazy::Ready(table) = &self.equal {
            if table.len() != 0 { return Err(invalid("Avro equality recursion is already active")); }
            for charge in table.leases() { context.retain(Arc::clone(charge))?; }
        }
        Ok(())
    }
    fn cleanup<T>(&mut self, hash: bool, first: bool, result: Result<T>) -> Result<T> {
        if !first { return result; }
        let cleanup = if hash {
            match &mut self.hash { Lazy::Ready(table) => table.clear(), Lazy::Cold => Ok(()) }
        } else {
            match &mut self.equal { Lazy::Ready(table) => table.clear(), Lazy::Cold => Ok(()) }
        };
        if cleanup.is_err() { self.failed = true; }
        match result {
            Err(cause) => Err(cause),
            Ok(value) => { cleanup?; Ok(value) },
        }
    }
}
/// Hash, compute and equality can return avro-native-traversal-limit beyond the native traversal depth limit of 128.
/// This partial native resource boundary has no corresponding fixed Avro/JVM depth limit.
pub(in crate::connect::avro) struct Evaluation<'a, W> {
    pub(super) authority: Authority<'a, W>,
    state: &'a mut RecursionState,
}
impl<'a, W: Witness> Evaluation<'a, W> {
    pub(in crate::connect::avro) fn new(state: &'a mut RecursionState, context: &'a mut ConversionContext, witness: &'a mut W) -> Result<Self> {
        state.retain(context)?;
        Ok(Self { authority: Authority { context, witness }, state })
    }
    pub(in crate::connect::avro) fn hash_code(&mut self, source: &SchemaHandle) -> Result<i32> {
        self.state.usable()?;
        self.hash(source, 0)
    }
    pub(in crate::connect::avro) fn compute_hash(&mut self, source: &SchemaHandle) -> Result<i32> {
        self.state.usable()?;
        self.compute(source, 0)
    }
    pub(in crate::connect::avro) fn equal(&mut self, left: &SchemaHandle, right: &SchemaHandle) -> Result<bool> {
        self.state.usable()?;
        self.equals(left, right, 0)
    }
    fn hash(&mut self, source: &SchemaHandle, depth: usize) -> Result<i32> {
        let node = source.view().node();
        let cached = node.hash.load(Ordering::Relaxed);
        if cached != NO_HASHCODE { return Ok(cached); }
        let hash = self.compute(source, depth)?;
        node.hash.store(hash, Ordering::Relaxed);
        Ok(hash)
    }
    fn base(&mut self, source: &SchemaHandle, depth: usize) -> Result<i32> {
        let node = source.view().node();
        let hash = self.authority.witness.type_hash(source_type(&node.kind), self.authority.context)?;
        Ok(hash.wrapping_add(node.properties.hash(self.authority.context, depth + 1)?))
    }
    fn compute(&mut self, source: &SchemaHandle, depth: usize) -> Result<i32> {
        guard_depth(depth, "Schema hash traversal exceeds the native traversal depth limit of 128")?;
        let node = source.view().node();
        if let Kind::Record { fields, name, .. } = &node.kind {
            if self.state.hash(&mut self.authority)?.contains_key(source, &mut SchemaIdentityOps, &mut self.authority)? { return Ok(0); }
            let first = self.state.hash(&mut self.authority)?.is_empty();
            let result = (|| {
                self.state.hash(&mut self.authority)?.put(source.clone(), (), &mut SchemaIdentityOps, &mut self.authority)?;
                let base = self.base(source, depth)?.wrapping_add(name.hash());
                let fields = fields.as_ref().ok_or_else(|| error("avro-schema-unchecked", "Record hash demanded unset fields"))?;
                let mut fields_hash = 1i32;
                for field in fields {
                    let hash = field.name.view().java_hash_code().wrapping_add(self.compute(&source.at(field.schema)?, depth + 1)?);
                    fields_hash = fields_hash.wrapping_mul(31).wrapping_add(hash);
                }
                Ok(base.wrapping_add(fields_hash))
            })();
            return self.state.cleanup(true, first, result);
        }
        let mut hash = self.base(source, depth)?;
        match &node.kind {
            Kind::Enum { name, symbols, .. } => {
                hash = hash.wrapping_add(name.hash());
                let symbols = symbols.iter().fold(1i32, |hash, symbol| hash.wrapping_mul(31).wrapping_add(symbol.view().java_hash_code()));
                hash = hash.wrapping_add(symbols);
            }
            Kind::Fixed { name, size } => hash = hash.wrapping_add(name.hash()).wrapping_add(*size),
            Kind::Array(child) | Kind::Map(child) => hash = hash.wrapping_add(self.compute(&source.at(*child)?, depth + 1)?),
            Kind::Union(children) => for child in children { hash = hash.wrapping_add(self.compute(&source.at(*child)?, depth + 1)?); },
            Kind::Primitive(_) => (),
            Kind::Record { .. } => unreachable!(),
        }
        Ok(hash)
    }
    fn equals(&mut self, left: &SchemaHandle, right: &SchemaHandle, depth: usize) -> Result<bool> {
        if left.same_object(right) { return Ok(true); }
        let left_node = left.view().node();
        let right_node = right.view().node();
        if source_type(&left_node.kind) != source_type(&right_node.kind) { return Ok(false); }
        let left_hash = left_node.hash.load(Ordering::Relaxed);
        let right_hash = right_node.hash.load(Ordering::Relaxed);
        if left_hash != right_hash && left_hash != NO_HASHCODE && right_hash != NO_HASHCODE { return Ok(false); }
        guard_depth(depth, "Schema equality traversal exceeds the native traversal depth limit of 128")?;
        match (&left_node.kind, &right_node.kind) {
            (Kind::Primitive(_), Kind::Primitive(_)) => left_node.properties.equal(&right_node.properties, self.authority.context, depth + 1),
            (Kind::Array(left_child), Kind::Array(right_child)) | (Kind::Map(left_child), Kind::Map(right_child)) => {
                Ok(self.equals(&left.at(*left_child)?, &right.at(*right_child)?, depth + 1)?
                    && left_node.properties.equal(&right_node.properties, self.authority.context, depth + 1)?)
            }
            (Kind::Union(left_children), Kind::Union(right_children)) => {
                for (left_child, right_child) in left_children.iter().zip(right_children) {
                    if !self.equals(&left.at(*left_child)?, &right.at(*right_child)?, depth + 1)? { return Ok(false); }
                }
                Ok(left_children.len() == right_children.len()
                    && left_node.properties.equal(&right_node.properties, self.authority.context, depth + 1)?)
            }
            (Kind::Enum { name: left_name, symbols: left_symbols, .. }, Kind::Enum { name: right_name, symbols: right_symbols, .. }) => {
                Ok(left_name == right_name && left_symbols == right_symbols
                    && left_node.properties.equal(&right_node.properties, self.authority.context, depth + 1)?)
            }
            (Kind::Fixed { name: left_name, size: left_size }, Kind::Fixed { name: right_name, size: right_size }) => {
                Ok(left_name == right_name && left_size == right_size
                    && left_node.properties.equal(&right_node.properties, self.authority.context, depth + 1)?)
            }
            (Kind::Record { name: left_name, fields: left_fields, .. }, Kind::Record { name: right_name, fields: right_fields, .. }) => {
                if left_name != right_name || !left_node.properties.equal(&right_node.properties, self.authority.context, depth + 1)? { return Ok(false); }
                let pair = SeenPair {
                    left: left.clone(), right: right.clone(),
                    id: PairId(NEXT_PAIR.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| next.checked_add(1)).map_err(|_| overflow())?),
                };
                if self.state.equal()?.contains_key(&pair, &mut SeenOps, &mut self.authority)? { return Ok(true); }
                let first = self.state.equal()?.len() == 0;
                let result = (|| {
                    self.state.equal()?.put(pair, (), &mut SeenOps, &mut self.authority)?;
                    match (left_fields, right_fields) {
                        (None, None) => Ok(true),
                        (Some(left_fields), Some(right_fields)) => {
                            for (left_field, right_field) in left_fields.iter().zip(right_fields) {
                                if !self.field_equal(left, left_field, right, right_field, depth + 1)? { return Ok(false); }
                            }
                            Ok(left_fields.len() == right_fields.len())
                        }
                        _ => Ok(false),
                    }
                })();
                self.state.cleanup(false, first, result)
            }
            _ => Ok(false),
        }
    }
    fn field_equal(&mut self, left_owner: &SchemaHandle, left: &Field, right_owner: &SchemaHandle, right: &Field, depth: usize) -> Result<bool> {
        if left.name != right.name || !self.equals(&left_owner.at(left.schema)?, &right_owner.at(right.schema)?, depth + 1)? { return Ok(false); }
        let defaults = match (&left.default, &right.default) {
            (None, None) => true,
            (Some(left), Some(right)) => if left.is_nan_double(self.authority.context)? {
                right.is_nan_double(self.authority.context)?
            } else {
                left.equal(right, self.authority.context, depth + 1)?
            },
            _ => false,
        };
        Ok(defaults && left.order == right.order && left.properties.equal(&right.properties, self.authority.context, depth + 1)?)
    }
}
impl<W: Witness> Admission for Evaluation<'_, W> {
    type Lease = Arc<MemoryReservation>;
    fn reserve(&mut self, bytes: usize) -> Result<Self::Lease> { self.authority.context.reserve(bytes) }
}

#[cfg(test)]
mod tests;
