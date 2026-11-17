use std::mem::{replace, size_of};
use rtdb_types::{Anomaly, Result, java_hash_map::Admission};

pub(super) trait IdentityOps<K, A: Admission> {
    fn same(&self, candidate: &K, stored: &K) -> bool;
    fn hash(&mut self, key: &K, admission: &mut A) -> Result<i32>;
}
pub(super) struct IdentityTable<K, V, L> { table: Vec<Option<Entry<K,V>>>, len: usize, charges: Vec<L>, failed: bool }
struct Entry<K,V> { key: K, value: V }
const MAXIMUM_CAPACITY: usize = 1 << 29;
fn invalid(message: &'static str) -> Anomaly { Anomaly::incorrect("rtdb/avro-identity-map", message) }
fn overflow() -> Anomaly { Anomaly::incorrect("rtdb/allocation-overflow", "Avro identity-map storage overflow") }
fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(overflow) }
fn mul(left: usize, right: usize) -> Result<usize> { left.checked_mul(right).ok_or_else(overflow) }
fn storage<T>(count: usize) -> Result<usize> { add(mul(mul(count, size_of::<T>())?, 16)?, 256) }
fn allocation(cause: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("rtdb/allocation-failed", cause.to_string()) }

impl<K,V,L> IdentityTable<K,V,L> {
    pub(super) fn new<A: Admission<Lease=L>>(admission: &mut A) -> Result<Self> {
        let mut table = Self { table: Vec::new(), len: 0, charges: Vec::new(), failed: false };
        table.admit(storage::<Option<Entry<K,V>>>(32)?, admission)?;
        table.table = Self::empty(32)?;
        Ok(table)
    }
    fn empty(count: usize) -> Result<Vec<Option<Entry<K,V>>>> {
        let mut table = Vec::new(); table.try_reserve_exact(count).map_err(allocation)?; table.resize_with(count, || None); Ok(table)
    }
    fn admit<A: Admission<Lease=L>>(&mut self, bytes: usize, admission: &mut A) -> Result<()> {
        let next = if self.charges.len() == self.charges.capacity() { Some(if self.charges.capacity() == 0 { 4 } else { mul(self.charges.capacity(), 2)? }) } else { None };
        let lease = admission.reserve(add(bytes, next.map_or(Ok(0), storage::<L>)?)?)?;
        if let Some(next) = next { self.charges.try_reserve_exact(next - self.charges.len()).map_err(allocation)?; }
        self.charges.push(lease); Ok(())
    }
    fn usable(&self) -> Result<()> { if self.failed { Err(invalid("A failed identity table cannot be reused")) } else { Ok(()) } }
    fn index(hash: i32, capacity: usize) -> usize {
        let shifted = hash.wrapping_shl(1).wrapping_sub(hash.wrapping_shl(8));
        (shifted as u32 as usize & (2 * capacity - 1)) / 2
    }
    fn next(index: usize, capacity: usize) -> usize { (index + 1) & (capacity - 1) }
    fn find<A: Admission<Lease=L>, O: IdentityOps<K,A>>(&self, key: &K, ops: &mut O, admission: &mut A) -> Result<Option<usize>> {
        let mut index = Self::index(ops.hash(key, admission)?, self.table.len());
        loop {
            match &self.table[index] { Some(entry) if ops.same(key, &entry.key) => return Ok(Some(index)), None => return Ok(None), _ => index = Self::next(index, self.table.len()) }
        }
    }
    pub(super) fn get<A: Admission<Lease=L>, O: IdentityOps<K,A>>(&self, key: &K, ops: &mut O, admission: &mut A) -> Result<Option<&V>> {
        self.usable()?; Ok(self.find(key, ops, admission)?.map(|index| &self.table[index].as_ref().unwrap().value))
    }
    pub(super) fn contains_key<A: Admission<Lease=L>, O: IdentityOps<K,A>>(&self, key: &K, ops: &mut O, admission: &mut A) -> Result<bool> { self.usable()?; Ok(self.find(key, ops, admission)?.is_some()) }
    pub(super) fn put<A: Admission<Lease=L>, O: IdentityOps<K,A>>(&mut self, key: K, value: V, ops: &mut O, admission: &mut A) -> Result<Option<V>> {
        self.usable()?;
        match self.put_inner(key, value, ops, admission) { Ok(previous) => Ok(previous), Err(cause) => { self.failed = true; Err(cause) } }
    }
    fn put_inner<A: Admission<Lease=L>, O: IdentityOps<K,A>>(&mut self, key: K, value: V, ops: &mut O, admission: &mut A) -> Result<Option<V>> {
        loop {
            let mut index = Self::index(ops.hash(&key, admission)?, self.table.len());
            while let Some(entry) = &mut self.table[index] {
                if ops.same(&key, &entry.key) { return Ok(Some(replace(&mut entry.value, value))); }
                index = Self::next(index, self.table.len());
            }
            let next_size = add(self.len, 1)?;
            if mul(next_size, 3)? > mul(self.table.len(), 2)? && self.resize(ops, admission)? { continue; }
            self.table[index] = Some(Entry { key, value }); self.len = next_size; return Ok(None);
        }
    }
    fn resize<A: Admission<Lease=L>, O: IdentityOps<K,A>>(&mut self, ops: &mut O, admission: &mut A) -> Result<bool> {
        let old_capacity = self.table.len();
        if old_capacity == MAXIMUM_CAPACITY {
            if self.len == MAXIMUM_CAPACITY - 1 { return Err(invalid("Identity table capacity exhausted")); }
            return Ok(false);
        }
        let capacity = mul(old_capacity, 2)?; self.admit(storage::<Option<Entry<K,V>>>(capacity)?, admission)?;
        let mut next = Self::empty(capacity)?;
        for old in &mut self.table {
            if let Some(entry) = old.take() {
                let mut index = Self::index(ops.hash(&entry.key, admission)?, capacity);
                while next[index].is_some() { index = Self::next(index, capacity); }
                next[index] = Some(entry);
            }
        }
        self.table = next; Ok(true)
    }
    pub(super) fn remove<A: Admission<Lease=L>, O: IdentityOps<K,A>>(&mut self, key: &K, ops: &mut O, admission: &mut A) -> Result<Option<V>> {
        self.usable()?;
        match self.remove_inner(key, ops, admission) { Ok(value) => Ok(value), Err(cause) => { self.failed = true; Err(cause) } }
    }
    fn remove_inner<A: Admission<Lease=L>, O: IdentityOps<K,A>>(&mut self, key: &K, ops: &mut O, admission: &mut A) -> Result<Option<V>> {
        let Some(mut deleted) = self.find(key, ops, admission)? else { return Ok(None); };
        self.len -= 1; let removed = self.table[deleted].take().unwrap();
        let mut index = Self::next(deleted, self.table.len());
        while let Some(entry) = &self.table[index] {
            let natural = Self::index(ops.hash(&entry.key, admission)?, self.table.len());
            if (index < natural && (natural <= deleted || deleted <= index)) || (natural <= deleted && deleted <= index) {
                self.table[deleted] = self.table[index].take(); deleted = index;
            }
            index = Self::next(index, self.table.len());
        }
        Ok(Some(removed.value))
    }
    pub(super) fn clear(&mut self) -> Result<()> { self.usable()?; for slot in &mut self.table { *slot = None; } self.len = 0; Ok(()) }
    pub(super) fn is_empty(&self) -> bool { self.len == 0 }
    pub(super) fn leases(&self) -> &[L] { &self.charges }
    pub(super) fn entries(&self) -> Result<impl Iterator<Item=(&K,&V)>> { self.usable()?; Ok(self.table.iter().filter_map(|slot|slot.as_ref().map(|entry|(&entry.key,&entry.value)))) }
}

#[cfg(test)]
mod tests;
