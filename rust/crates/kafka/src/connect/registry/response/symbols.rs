use std::{alloc::Layout, mem::size_of, sync::{Arc, Mutex, atomic::AtomicUsize}};
use rtdb_types::{Anomaly, MemoryReservation, java_text::JavaString};
use super::ConversionContext;
pub(in crate::connect) use super::Profile;

mod storage;
mod intern;
pub(in crate::connect) use intern::InternCache;
use storage::{Bucket, OverflowBits, Table};

const INITIAL_SIZE: usize = 64;
const MAX_SIZE: usize = 65_536;
const REUSE_LIMIT: usize = 12_000;
const COLLISION_LIMIT: usize = 150;
const NAME_LIMIT: usize = 50_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::connect) struct FactoryProfile { pub profile: Profile, pub identity_hash_seed: i32 }

#[derive(Debug)]
pub(in crate::connect) enum Failure {
    Native(Anomaly),
    NameLength(usize),
    CollisionOverflow { count: usize },
    RehashCount { expected: usize, actual: usize },
    Poisoned,
}
impl From<Anomaly> for Failure { fn from(cause: Anomaly) -> Self { Self::Native(cause) } }
pub(in crate::connect) type Result<T> = std::result::Result<T, Failure>;

pub(in crate::connect) struct SymbolName { text: JavaString, charge: Arc<MemoryReservation> }
impl SymbolName {
    fn copy(units: &[u16], context: &mut ConversionContext) -> Result<Arc<Self>> {
        let bytes = JavaString::allocation_bound_utf16(units)?.checked_add(shared_bound::<Self>()?).ok_or_else(overflow)?;
        let charge = context.reserve(bytes)?;
        let text = JavaString::copy_utf16_with_admission(units, |_| Ok(Arc::clone(&charge)))?;
        Ok(Arc::new(Self { text, charge }))
    }
    pub(in crate::connect) fn text(&self) -> &JavaString { &self.text }
    pub(in crate::connect) fn retain(&self, context: &mut ConversionContext) -> rtdb_types::Result<()> { context.retain(Arc::clone(&self.charge)) }
}

pub(in crate::connect) trait InternAuthority {
    fn intern(&mut self, candidate: Arc<SymbolName>, context: &mut ConversionContext) -> rtdb_types::Result<Arc<SymbolName>>;
}

pub(in crate::connect) struct ReaderSymbols {
    factory: FactoryProfile, state: Mutex<Arc<Table>>, empty: Arc<SymbolName>, charge: Arc<MemoryReservation>,
}
impl ReaderSymbols {
    pub(in crate::connect) fn factory(&self) -> FactoryProfile { self.factory }
    pub(in crate::connect) fn new(factory: FactoryProfile, context: &mut ConversionContext) -> Result<Arc<Self>> {
        let charge = context.reserve(shared_bound::<Self>()?)?;
        let state = Table::new(INITIAL_SIZE, context)?;
        let empty = SymbolName::copy(&[], context)?;
        Ok(Arc::new(Self { factory, state: Mutex::new(Arc::new(state)), empty, charge }))
    }
    pub(in crate::connect) fn child(self: &Arc<Self>, context: &mut ConversionContext) -> Result<ChildSymbols> {
        context.retain(Arc::clone(&self.charge))?;
        self.empty.retain(context)?;
        let table = self.state.lock().map_err(|_| poisoned_lock())?;
        table.retain(context)?;
        Ok(ChildSymbols { root: Arc::clone(self), state: ChildState::Active(Active {
            storage: Storage::Shared(Arc::clone(&table)), canonicalize: true, overflows: None,
        }) })
    }
    fn compare_publish(&self, previous: &Arc<Table>, replacement: Arc<Table>) -> Result<bool> {
        let mut current = self.state.lock().map_err(|_| poisoned_lock())?;
        if !Arc::ptr_eq(&current, previous) { return Ok(false); }
        *current = replacement;
        Ok(true)
    }
}

enum Storage { Shared(Arc<Table>), Private(Table) }
impl Storage { fn table(&self) -> &Table { match self { Self::Shared(table) => table, Self::Private(table) => table } } }
struct Active { storage: Storage, canonicalize: bool, overflows: Option<OverflowBits> }
enum ChildState { Active(Active), Poisoned }
/// Use the creation ConversionContext for every lookup and release of this parser child.
pub(in crate::connect) struct ChildSymbols { state: ChildState, root: Arc<ReaderSymbols> }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::connect) enum Release { Unchanged, Published, Reset, LostPublication, Disabled, Poisoned }

impl ChildSymbols {
    #[cfg(test)]
    pub(in crate::connect) fn factory(&self) -> FactoryProfile { self.root.factory }
    pub(in crate::connect) fn find(&mut self, units: &[u16], authority: &mut (impl InternAuthority + ?Sized),
        context: &mut ConversionContext) -> Result<JavaString> {
        let result = self.find_inner(units, authority, context);
        if matches!(&result, Err(Failure::Native(_) | Failure::Poisoned)) { self.state = ChildState::Poisoned; }
        result
    }
    fn find_inner(&mut self, units: &[u16], authority: &mut (impl InternAuthority + ?Sized),
        context: &mut ConversionContext) -> Result<JavaString> {
        let ChildState::Active(active) = &mut self.state else { return Err(Failure::Poisoned); };
        if units.is_empty() { return Ok(self.root.empty.text.clone()); }
        if !active.canonicalize {
            validate_length(units)?;
            return Ok(JavaString::copy_utf16_with_admission(units, |bytes| context.reserve(bytes))?);
        }
        let raw = parser_hash(self.root.factory.identity_hash_seed, units);
        let index = mixed_index(raw, active.storage.table().primary.len());
        let table = active.storage.table();
        if let Some(name) = &table.primary[index] {
            if name.text.view().units().eq(units.iter().copied()) { return Ok(name.text.clone()); }
            let mut bucket = table.buckets[index >> 1].as_deref();
            while let Some(entry) = bucket {
                if entry.name.text.view().units().eq(units.iter().copied()) { return Ok(entry.name.text.clone()); }
                bucket = entry.next.as_deref();
            }
        }
        validate_length(units)?;
        let was_shared = matches!(&active.storage, Storage::Shared(_));
        if was_shared {
            active.storage = Storage::Private(active.storage.table().copy(context)?);
        }
        let mut index = index;
        let Storage::Private(table) = &mut active.storage else { unreachable!() };
        if !was_shared && table.count >= table.threshold {
            rehash(table, &mut active.canonicalize, &mut active.overflows, self.root.factory.identity_hash_seed, context)?;
            let rehash_hash = normalized_hash(self.root.factory.identity_hash_seed, units);
            index = mixed_index(rehash_hash, table.primary.len());
        }
        let candidate = SymbolName::copy(units, context)?;
        let name = authority.intern(Arc::clone(&candidate), context)?;
        if !Arc::ptr_eq(&name, &candidate) { name.retain(context)?; }
        if !name.text.view().units().eq(units.iter().copied()) {
            return Err(Anomaly::unsupported("xtdb.kafka-connect-source/registry-symbol-intern", "Intern authority changed the symbol's UTF16 content").into());
        }
        table.count += 1;
        if table.primary[index].is_none() { table.primary[index] = Some(Arc::clone(&name)); }
        else {
            let bucket_index = index >> 1;
            let entry = Bucket::new(Arc::clone(&name), table.buckets[bucket_index].clone(), context)?;
            if entry.length > COLLISION_LIMIT {
                if let Some(bits) = &active.overflows {
                    if bits.get(bucket_index) { return Err(Failure::CollisionOverflow { count: table.count }); }
                } else { active.overflows = Some(OverflowBits::new(context)?); }
                active.overflows.as_mut().unwrap().set(bucket_index, context)?;
                table.primary[index] = Some(Arc::clone(&entry.name));
                table.buckets[bucket_index] = None;
                table.count -= entry.length;
                table.longest = -1;
            } else {
                table.longest = table.longest.max(entry.length as i32);
                table.buckets[bucket_index] = Some(entry);
            }
        }
        Ok(name.text.clone())
    }

    pub(in crate::connect) fn release(self, context: &mut ConversionContext) -> Result<Release> {
        let ChildState::Active(active) = self.state else { return Ok(Release::Poisoned); };
        if !active.canonicalize { return Ok(Release::Disabled); }
        let Storage::Private(table) = active.storage else { return Ok(Release::Unchanged); };
        context.retain(Arc::clone(&self.root.charge))?;
        let previous = {
            let guard = self.root.state.lock().map_err(|_| poisoned_lock())?;
            guard.retain(context)?;
            Arc::clone(&guard)
        };
        if previous.count == table.count { return Ok(Release::Unchanged); }
        let reset = table.count > REUSE_LIMIT;
        let replacement = if reset { Arc::new(Table::new(INITIAL_SIZE, context)?) } else { Arc::new(table) };
        if !self.root.compare_publish(&previous, replacement)? { return Ok(Release::LostPublication); }
        Ok(if reset { Release::Reset } else { Release::Published })
    }
}

fn rehash(table: &mut Table, canonicalize: &mut bool, overflows: &mut Option<OverflowBits>, seed: i32,
    context: &mut ConversionContext) -> Result<()> {
    let new_size = table.primary.len().checked_mul(2).ok_or_else(overflow)?;
    if new_size > MAX_SIZE {
        let mut replacement = Table::new(INITIAL_SIZE, context)?;
        replacement.threshold = table.threshold;
        replacement.longest = table.longest;
        *table = replacement;
        *canonicalize = false;
        return Ok(());
    }
    let mut replacement = Table::new(new_size, context)?;
    replacement.count = table.count;
    let mut count = 0usize;
    for name in table.primary.iter().flatten() {
        count += 1;
        rehash_name(&mut replacement, name, seed, context)?;
    }
    for head in &table.buckets {
        let mut bucket = head.as_deref();
        while let Some(entry) = bucket {
            count += 1;
            rehash_name(&mut replacement, &entry.name, seed, context)?;
            bucket = entry.next.as_deref();
        }
    }
    *table = replacement;
    *overflows = None;
    if count != table.count { return Err(Failure::RehashCount { expected: table.count, actual: count }); }
    Ok(())
}
fn rehash_name(table: &mut Table, name: &Arc<SymbolName>, seed: i32, context: &mut ConversionContext) -> Result<()> {
    let hash = name.text.view().units().fold(seed, |hash, unit| hash.wrapping_mul(33).wrapping_add(i32::from(unit)));
    let index = mixed_index(if hash == 0 { 1 } else { hash }, table.primary.len());
    if table.primary[index].is_none() { table.primary[index] = Some(Arc::clone(name)); }
    else {
        let bucket_index = index >> 1;
        let entry = Bucket::new(Arc::clone(name), table.buckets[bucket_index].clone(), context)?;
        table.longest = table.longest.max(entry.length as i32);
        table.buckets[bucket_index] = Some(entry);
    }
    Ok(())
}
fn parser_hash(seed: i32, units: &[u16]) -> i32 { units.iter().fold(seed, |hash, unit| hash.wrapping_mul(33).wrapping_add(i32::from(*unit))) }
fn normalized_hash(seed: i32, units: &[u16]) -> i32 { match parser_hash(seed, units) { 0 => 1, hash => hash } }
fn mixed_index(hash: i32, length: usize) -> usize {
    let mut hash = hash as u32;
    hash = hash.wrapping_add(hash >> 15);
    hash ^= hash << 7;
    hash = hash.wrapping_add(hash >> 3);
    hash as usize & (length - 1)
}
fn validate_length(units: &[u16]) -> Result<()> { if units.len() > NAME_LIMIT { Err(Failure::NameLength(units.len())) } else { Ok(()) } }
fn shared_bound<T>() -> rtdb_types::Result<usize> {
    let layout = Layout::new::<[AtomicUsize; 2]>().extend(Layout::new::<T>()).map_err(|_| overflow())?.0.pad_to_align();
    layout.size().checked_add(256).ok_or_else(overflow)
}
fn overflow() -> Anomaly { Anomaly::incorrect("xtdb.kafka-connect-source/allocation-overflow", "Registry symbol allocation exceeds addressable memory") }
fn allocation_failed() -> Anomaly { Anomaly::incorrect("xtdb.kafka-connect-source/allocation-failed", "Registry symbol allocation failed") }
fn poisoned_lock() -> Anomaly { Anomaly::fault("xtdb.kafka-connect-source/registry-symbol-lock", "Registry symbol publication lock was poisoned") }

#[cfg(test)]
mod tests;
