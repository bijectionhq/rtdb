use std::sync::{Arc, Mutex};
use rtdb_types::{Anomaly, MemoryReservation, java_text::JavaString};
use super::{ConversionContext, InternAuthority, Profile, SymbolName, allocation_failed, overflow, poisoned_lock, shared_bound};

mod storage;
use storage::Table;

const INITIAL_SIZE: usize = 64;
const MAX_SIZE: usize = 65_536;
const REUSE_LIMIT: usize = 6000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::connect) struct FactoryProfile { pub profile: Profile, pub seed: i32 }

#[derive(Debug)]
pub(in crate::connect) enum Failure {
    Native(Anomaly),
    CollisionOverflow { count: usize, size: usize },
    LongNameOffset { start: usize, length: usize, profile: Profile },
    ArrayLength { original: usize, added: usize },
    RehashCount { expected: usize, actual: usize },
    Poisoned,
}
impl From<Anomaly> for Failure { fn from(cause: Anomaly) -> Self { Self::Native(cause) } }
pub(in crate::connect) type Result<T> = std::result::Result<T, Failure>;

#[derive(Clone, Copy)]
pub(in crate::connect) struct Quads<'a>(&'a [u32]);
impl<'a> Quads<'a> {
    pub(in crate::connect) fn new(values: &'a [u32]) -> Option<Self> {
        if values.is_empty() || values.len() > i32::MAX as usize { None } else { Some(Self(values)) }
    }
}

pub(in crate::connect) fn prepare_name(units: &[u16], context: &mut ConversionContext) -> Result<Arc<SymbolName>> {
    SymbolName::copy(units, context).map_err(|cause| match cause { super::Failure::Native(cause) => Failure::Native(cause), _ => unreachable!() })
}

pub(in crate::connect) struct ByteSymbols {
    factory: FactoryProfile, state: Mutex<Arc<Table>>, empty: Arc<SymbolName>, charge: Arc<MemoryReservation>,
}
impl ByteSymbols {
    pub(in crate::connect) fn new(factory: FactoryProfile, context: &mut ConversionContext) -> Result<Arc<Self>> {
        let charge = context.reserve(shared_bound::<Self>()?)?;
        let table = Table::new(INITIAL_SIZE, INITIAL_SIZE * 8, INITIAL_SIZE * 2, context)?;
        let empty = prepare_name(&[], context)?;
        Ok(Arc::new(Self { factory, state: Mutex::new(Arc::new(table)), empty, charge }))
    }
    pub(in crate::connect) fn factory(&self) -> FactoryProfile { self.factory }
    pub(in crate::connect) fn child(self: &Arc<Self>, context: &mut ConversionContext) -> Result<ByteChild> {
        context.retain(Arc::clone(&self.charge))?;
        self.empty.retain(context)?;
        let table = self.state.lock().map_err(|_| poisoned_lock())?;
        table.retain(context)?;
        Ok(ByteChild { root: Arc::clone(self), state: ChildState::Shared(Arc::clone(&table)) })
    }
    fn compare_publish(&self, previous: &Arc<Table>, replacement: Arc<Table>) -> Result<bool> {
        let mut current = self.state.lock().map_err(|_| poisoned_lock())?;
        if !Arc::ptr_eq(&current, previous) { return Ok(false); }
        *current = replacement;
        Ok(true)
    }
}

enum ChildState { Shared(Arc<Table>), Private(Table), Poisoned }
/// Use the creation ConversionContext for name preparation, additions and release.
pub(in crate::connect) struct ByteChild { state: ChildState, root: Arc<ByteSymbols> }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::connect) enum Release { Unchanged, Published, Reset, LostPublication, Poisoned }

impl ByteChild {
    pub(in crate::connect) fn factory(&self) -> FactoryProfile { self.root.factory }
    pub(in crate::connect) fn find(&self, quads: &[u32]) -> Result<Option<JavaString>> {
        let table = match &self.state { ChildState::Shared(table) => table.as_ref(), ChildState::Private(table) => table, ChildState::Poisoned => return Err(Failure::Poisoned) };
        if quads.len() > i32::MAX as usize { return Err(Anomaly::unsupported("xtdb.kafka-connect-source/unsupported-source-profile", "Byte quad length exceeds the Java source argument range").into()); }
        if quads.is_empty() { return Ok(Some(self.root.empty.text.clone())); }
        Ok(table.find(quads, hash(self.root.factory.seed, quads)).map(|name| name.text.clone()))
    }
    pub(in crate::connect) fn add(&mut self, quads: Quads<'_>, name: Arc<SymbolName>, authority: &mut (impl InternAuthority + ?Sized),
        context: &mut ConversionContext) -> Result<JavaString> {
        let result = self.add_inner(quads, name, authority, context);
        if matches!(&result, Err(Failure::Native(_) | Failure::Poisoned)) { self.state = ChildState::Poisoned; }
        result
    }
    fn add_inner(&mut self, quads: Quads<'_>, name: Arc<SymbolName>, authority: &mut (impl InternAuthority + ?Sized),
        context: &mut ConversionContext) -> Result<JavaString> {
        if let ChildState::Shared(table) = &self.state { self.state = ChildState::Private(table.copy(context)?); }
        let ChildState::Private(table) = &mut self.state else { return Err(Failure::Poisoned); };
        let result = add(table, quads.0, name, self.root.factory, authority, context)?;
        Ok(result.text.clone())
    }
    pub(in crate::connect) fn release(self, context: &mut ConversionContext) -> Result<Release> {
        let table = match self.state { ChildState::Shared(_) => return Ok(Release::Unchanged), ChildState::Poisoned => return Ok(Release::Poisoned), ChildState::Private(table) => table };
        context.retain(Arc::clone(&self.root.charge))?;
        let previous = {
            let current = self.root.state.lock().map_err(|_| poisoned_lock())?;
            current.retain(context)?;
            Arc::clone(&current)
        };
        if table.count == previous.count { return Ok(Release::Unchanged); }
        let reset = table.count > REUSE_LIMIT;
        let replacement = if reset { Arc::new(Table::new(INITIAL_SIZE, INITIAL_SIZE * 8, INITIAL_SIZE * 2, context)?) } else { Arc::new(table) };
        if !self.root.compare_publish(&previous, replacement)? { return Ok(Release::LostPublication); }
        Ok(if reset { Release::Reset } else { Release::Published })
    }
}

fn intern(name: Arc<SymbolName>, authority: &mut (impl InternAuthority + ?Sized), context: &mut ConversionContext) -> Result<Arc<SymbolName>> {
    let result = authority.intern(Arc::clone(&name), context)?;
    if !Arc::ptr_eq(&name, &result) { result.retain(context)?; }
    if !name.text.view().units().eq(result.text.view().units()) {
        return Err(Anomaly::unsupported("xtdb.kafka-connect-source/registry-symbol-intern", "Intern authority changed the symbol's UTF16 content").into());
    }
    Ok(result)
}

fn add(table: &mut Table, quads: &[u32], name: Arc<SymbolName>, factory: FactoryProfile,
    authority: &mut (impl InternAuthority + ?Sized), context: &mut ConversionContext) -> Result<Arc<SymbolName>> {
    let name = intern(name, authority, context)?;
    let hash = hash(factory.seed, quads);
    let offset = find_offset(table, hash, factory, authority, context)?;
    if quads.len() <= 3 {
        table.area.values[offset..offset + quads.len()].copy_from_slice(quads);
    } else {
        table.area.values[offset] = hash;
        let start = table.append_long(quads, factory.profile, context)?;
        table.area.values[offset + 1] = start as u32;
    }
    table.area.values[offset + 3] = quads.len() as u32;
    table.names.values[offset >> 2] = Some(Arc::clone(&name));
    table.count += 1;
    Ok(name)
}

fn find_offset(table: &mut Table, hash: u32, factory: FactoryProfile,
    authority: &mut (impl InternAuthority + ?Sized), context: &mut ConversionContext) -> Result<usize> {
    let primary = table.primary(hash);
    if table.area.values[primary + 3] == 0 { return Ok(primary); }
    if table.needs_rehash(factory.profile) {
        rehash(table, factory, authority, context)?;
        return Ok(table.offset_after_rehash(hash));
    }
    if let Some(offset) = table.secondary_or_tertiary(primary) { return Ok(offset); }
    let offset = table.spill_end;
    table.spill_end += 4;
    if table.spill_end >= table.size * 8 {
        if table.size > 1024 { return Err(Failure::CollisionOverflow { count: table.count, size: table.size }); }
        rehash(table, factory, authority, context)?;
        return Ok(table.offset_after_rehash(hash));
    }
    Ok(offset)
}

fn rehash(table: &mut Table, factory: FactoryProfile, authority: &mut (impl InternAuthority + ?Sized), context: &mut ConversionContext) -> Result<()> {
    let size = table.size * 2;
    if size > MAX_SIZE { table.clear(); return Ok(()); }
    let area_length = expanded_length(table.area.values.len(), table.size * 8)?;
    let replacement = Table::new(size, area_length, table.names.values.len() * 2, context)?;
    let old = std::mem::replace(table, replacement);
    let mut copied = 0;
    for offset in (0..old.spill_end).step_by(4) {
        let length = old.area.values[offset + 3] as usize;
        if length == 0 { continue; }
        copied += 1;
        let quads = if length <= 3 { &old.area.values[offset..offset + length] }
            else { let start = old.area.values[offset + 1] as usize; &old.area.values[start..start + length] };
        let name = Arc::clone(old.names.values[offset >> 2].as_ref().unwrap());
        add(table, quads, name, factory, authority, context)?;
    }
    if copied != old.count { return Err(Failure::RehashCount { expected: old.count, actual: copied }); }
    Ok(())
}

fn hash(seed: i32, quads: &[u32]) -> u32 {
    match quads {
        [] => 0,
        [first] => {
            let mut value = *first ^ seed as u32;
            value = value.wrapping_add(value >> 16); value ^= value << 3; value.wrapping_add(value >> 12)
        }
        [first, second] => {
            let mut value = first.wrapping_add(*first >> 15); value ^= value >> 9;
            value = value.wrapping_add(second.wrapping_mul(33)); value ^= seed as u32;
            value = value.wrapping_add(value >> 16); value ^= value >> 4; value.wrapping_add(value << 3)
        }
        [first, second, third] => {
            let mut value = *first ^ seed as u32; value = value.wrapping_add(value >> 9); value = value.wrapping_mul(31);
            value = value.wrapping_add(*second).wrapping_mul(33); value = value.wrapping_add(value >> 15); value ^= *third;
            value = value.wrapping_add(value >> 4); value = value.wrapping_add(value >> 15); value ^ (value << 9)
        }
        _ => {
            let mut value = quads[0] ^ seed as u32; value = value.wrapping_add(value >> 9); value = value.wrapping_add(quads[1]);
            value = value.wrapping_add(value >> 15).wrapping_mul(33); value ^= quads[2]; value = value.wrapping_add(value >> 4);
            for next in &quads[3..] { value = value.wrapping_add(*next ^ ((*next as i32 >> 21) as u32)); }
            value = value.wrapping_mul(65599); value = value.wrapping_add(value >> 19); value ^ (value << 5)
        }
    }
}

fn expanded_length(original: usize, added: usize) -> Result<usize> {
    let length = original.checked_add(added).ok_or_else(overflow)?;
    if length > i32::MAX as usize { return Err(Failure::ArrayLength { original, added }); }
    Ok(length)
}

#[cfg(test)]
mod tests;
