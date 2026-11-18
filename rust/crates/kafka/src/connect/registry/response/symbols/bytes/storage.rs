use std::{alloc::Layout, mem::size_of};
use super::*;

pub(super) struct Area<T> { pub values: Vec<T>, charge: Arc<MemoryReservation> }
impl<T: Default + Clone> Area<T> {
    fn new(length: usize, context: &mut ConversionContext) -> Result<Self> {
        Layout::array::<T>(length).map_err(|_| overflow())?;
        let bytes = length.checked_mul(size_of::<T>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?;
        let charge = context.reserve(bytes)?;
        let mut values = Vec::new();
        values.try_reserve_exact(length).map_err(|_| allocation_failed())?;
        values.resize(length, T::default());
        Ok(Self { values, charge })
    }
    fn copy(&self, context: &mut ConversionContext) -> Result<Self> {
        let mut result = Self::new(self.values.len(), context)?;
        result.values.clone_from_slice(&self.values);
        Ok(result)
    }
}

pub(super) struct Table {
    pub size: usize, pub count: usize, pub tertiary_shift: u32, pub spill_end: usize, pub long_end: usize,
    pub area: Area<u32>, pub names: Area<Option<Arc<SymbolName>>>, charge: Arc<MemoryReservation>,
}
impl Table {
    pub fn new(size: usize, area_length: usize, names_length: usize, context: &mut ConversionContext) -> Result<Self> {
        let charge = context.reserve(shared_bound::<Self>()?)?;
        let area = Area::new(area_length, context)?;
        let names = Area::new(names_length, context)?;
        Ok(Self { size, count: 0, tertiary_shift: tertiary_shift(size), spill_end: size * 7, long_end: size * 8, area, names, charge })
    }
    pub fn copy(&self, context: &mut ConversionContext) -> Result<Self> {
        let charge = context.reserve(shared_bound::<Self>()?)?;
        let area = self.area.copy(context)?;
        let names = self.names.copy(context)?;
        Ok(Self { size: self.size, count: self.count, tertiary_shift: self.tertiary_shift,
            spill_end: self.spill_end, long_end: self.long_end, area, names, charge })
    }
    pub fn retain(&self, context: &mut ConversionContext) -> rtdb_types::Result<()> {
        context.retain(Arc::clone(&self.charge))?;
        context.retain(Arc::clone(&self.area.charge))?;
        context.retain(Arc::clone(&self.names.charge))?;
        for name in self.names.values.iter().flatten() { name.retain(context)?; }
        Ok(())
    }
    pub fn primary(&self, hash: u32) -> usize { (hash as usize & (self.size - 1)) * 4 }
    fn secondary(&self, primary: usize) -> usize { self.size * 4 + (primary >> 3) * 4 }
    fn tertiary(&self, primary: usize) -> usize { self.size * 6 + ((primary >> (self.tertiary_shift + 2)) << self.tertiary_shift) }
    pub fn secondary_or_tertiary(&self, primary: usize) -> Option<usize> {
        let secondary = self.secondary(primary);
        if self.area.values[secondary + 3] == 0 { return Some(secondary); }
        let start = self.tertiary(primary);
        (start..start + (1 << self.tertiary_shift)).step_by(4).find(|offset| self.area.values[offset + 3] == 0)
    }
    pub fn offset_after_rehash(&mut self, hash: u32) -> usize {
        let primary = self.primary(hash);
        if self.area.values[primary + 3] == 0 { return primary; }
        if let Some(offset) = self.secondary_or_tertiary(primary) { return offset; }
        let offset = self.spill_end;
        self.spill_end += 4;
        offset
    }
    pub fn needs_rehash(&self, profile: Profile) -> bool {
        if self.count <= self.size >> 1 { return false; }
        let spills = (self.spill_end - self.size * 7) >> 2;
        spills > ((1 + self.count) >> 7) || match profile {
            Profile::Jackson216 => self.count as f64 > self.size as f64 * 0.80,
            Profile::Jackson221 => self.count > ((self.size as u64 * 3_435_973_837) >> 32) as usize,
        }
    }
    pub fn find(&self, quads: &[u32], hash: u32) -> Option<&Arc<SymbolName>> {
        let primary = self.primary(hash);
        if self.matches(primary, quads, hash) { return self.names.values[primary >> 2].as_ref(); }
        if self.area.values[primary + 3] == 0 { return None; }
        let secondary = self.secondary(primary);
        if self.matches(secondary, quads, hash) { return self.names.values[secondary >> 2].as_ref(); }
        if quads.len() <= 3 && self.area.values[secondary + 3] == 0 { return None; }
        let start = self.tertiary(primary);
        for offset in (start..start + (1 << self.tertiary_shift)).step_by(4) {
            if self.matches(offset, quads, hash) { return self.names.values[offset >> 2].as_ref(); }
            if self.area.values[offset + 3] == 0 { return None; }
        }
        for offset in (self.size * 7..self.spill_end).step_by(4) {
            if self.matches(offset, quads, hash) { return self.names.values[offset >> 2].as_ref(); }
        }
        None
    }
    fn matches(&self, offset: usize, quads: &[u32], hash: u32) -> bool {
        if self.area.values[offset + 3] as usize != quads.len() { return false; }
        if quads.len() <= 3 { return self.area.values[offset..offset + quads.len()] == *quads; }
        if self.area.values[offset] != hash { return false; }
        let start = self.area.values[offset + 1] as usize;
        self.area.values[start..start + quads.len()] == *quads
    }
    pub fn append_long(&mut self, quads: &[u32], profile: Profile, context: &mut ConversionContext) -> Result<usize> {
        let start = self.long_end;
        let end = start.checked_add(quads.len()).ok_or_else(overflow)?;
        if end > i32::MAX as usize { return Err(Failure::LongNameOffset { start, length: quads.len(), profile }); }
        if end > self.area.values.len() {
            let added = (end - self.area.values.len()).max(self.size.min(4096));
            let length = expanded_length(self.area.values.len(), added)?;
            let mut replacement = Area::new(length, context)?;
            replacement.values[..self.area.values.len()].copy_from_slice(&self.area.values);
            self.area = replacement;
        }
        self.area.values[start..end].copy_from_slice(quads);
        self.long_end = end;
        Ok(start)
    }
    pub fn clear(&mut self) {
        self.count = 0; self.spill_end = self.size * 7; self.long_end = self.size * 8;
        self.area.values.fill(0); self.names.values.fill(None);
    }
}

fn tertiary_shift(size: usize) -> u32 {
    match size >> 2 { 0..64 => 4, 64..=256 => 5, 257..=1024 => 6, _ => 7 }
}
